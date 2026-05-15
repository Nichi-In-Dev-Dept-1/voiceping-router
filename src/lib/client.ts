import * as cluster from "cluster";
import * as EventEmitter from "events";

import * as dbug from "debug";
import * as Q from "q";
import * as WebSocket from "ws";

import ChannelType = require("./channeltype");
import config = require("./config");
import Connection from "./connection";
import logger = require("./logger");
import MessageType = require("./messagetype");
import Recorder from "./recorder";
import Redis = require("./redis");
import { IServer } from "./server";
import States from "./states";
import { IMessage, numberOrString } from "./types";

const dbug1 = dbug("vp:client");
function debug(msg: string) {
  dbug1((cluster.worker ? `worker ${cluster.worker.id} ` : "") + msg);
}

const PING_INTERVAL: number = config.pingInterval;
const PING_TIMEOUT: number = config.pingTimeout;
const OVERLAP_MISSED_CALL_DEDUPE_TTL_MS = 30000;

interface IConnections {
  [index: string]: Connection;
}

interface ITextMessageMeta {
  callId?: string;
  errorType?: string;
  membersInCall?: number;
  textMessageType?: string;
  translate?: boolean;
  lang?: string;
}

export default class Client extends EventEmitter {

  public id: numberOrString;
  private user: any;
  private pingInterval: NodeJS.Timer;
  private connections: IConnections = {};
  private server: IServer;
  // Track in-flight START operations so a STOP that arrives before floor acquisition
  // completes (race condition on quick tap-and-release) can be buffered and replayed.
  private pendingGroupStart: Map<string, IMessage> = new Map();
  private pendingGroupStop: Map<string, IMessage> = new Map();
  private pendingPrivateStart: Map<string, IMessage> = new Map();
  private pendingPrivateStop: Map<string, IMessage> = new Map();
  private overlapMissedCallKeys: Set<string> = new Set();
  private overlapMissedCallKeyTimers: Map<string, NodeJS.Timer> = new Map();

  constructor(id: numberOrString, user: any, server: IServer) {
    super();
    this.id = id;
    this.user = user;
    this.server = server;
  }

  public registerSocket(this: Client, socket: WebSocket, key: string, deviceId: string) {
    const connection = new Connection(key, socket, deviceId, this.id);
    connection.addListener("close", this.handleConnectionClose);
    connection.addListener("message", this.handleConnectionMessage);
    connection.addListener("pong", this.handleConnectionPong);
    this.connections[key] = connection;

    // Clear stale pending operations from the previous session. After a WiFi
    // drop-and-reconnect the Client object is reused and these maps may still
    // hold a STOP from the old session. If a new START arrives while the floor
    // is being acquired (async) the stale STOP gets buffered and replays 400ms
    // later, immediately killing the new call.
    this.pendingPrivateStart.clear();
    this.pendingPrivateStop.clear();
    this.pendingGroupStart.clear();
    this.pendingGroupStop.clear();

    // Clear user state on new connection to prevent stale busy/floor ownership states.
    // IMPORTANT: capture the private-call peer BEFORE wiping in-memory state so we can
    // also clear the peer's side.  Reading peer AFTER clearUserActiveCall would find
    // userPrivateCallState[id] already deleted and never reach the peer cleanup.
    const stalePeer = States.getPrivateCallPeer(this.id);
    // Only remove from groups where the user does NOT hold the floor.
    // If they hold the floor they are currently talking — removing them would end
    // their transmission when a listener reconnects.
    States.getGroupsWithActiveParticipant(this.id, (groupErr, activeGroups) => {
      if (activeGroups && activeGroups.length > 0) {
        const groupsToKeep: Array<number|string> = [];
        let groupsChecked = 0;
        activeGroups.forEach((groupId) => {
          States.isFloorOwnerOfGroup(groupId, this.id, (floorErr, isOwner) => {
            if (isOwner) {
              logger.info(`registerSocket: user ${this.id} holds group floor in ${groupId}` +
                          ` — keeping as active participant for reconnection`);
              groupsToKeep.push(groupId);
            }
            groupsChecked++;
            if (groupsChecked === activeGroups.length) {
              // Remove from all groups, then re-add groups where user holds floor.
              // Preserve the SOS flag from groupSosState so a reconnecting SOS caller
              // doesn't appear as isSos=false and get overridden by a second SOS.
              States.removeActiveParticipantFromAllGroups(this.id, (removeErr) => {
                groupsToKeep.forEach((groupToKeep) => {
                  States.addUserToActiveCallGroup(this.id, groupToKeep, States.isGroupSos(groupToKeep));
                });
              });
            }
          });
        });
      } else {
        States.removeActiveParticipantFromAllGroups(this.id);
      }
    });
    States.releaseFloorOwnershipForUser(this.id);
    States.releasePrivateFloorOwnershipForUser(this.id);
    States.clearUserActiveCall(this.id);
    if (stalePeer) {
      // Only clear the peer's call state if they are not the active floor owner.
      // If the peer holds the floor they are currently talking — wiping their state would
      // end their transmission when the listener reconnects.
      States.isPrivateFloorOwner(stalePeer, this.id, stalePeer, (floorErr, peerIsOwner) => {
        if (!peerIsOwner) {
          logger.info(`registerSocket: clearing stale private call peer ${stalePeer} for reconnecting user ${this.id}`);
          States.clearUserPrivateCall(stalePeer);
        } else {
          logger.info(`registerSocket: peer ${stalePeer} holds private floor —` +
                      ` skipping stale clear for reconnecting user ${this.id}`);
        }
      });
    }

    this.isLoginDuplicated(deviceId, key, (err, data) => {
      // Guard against race condition: if two sockets connect simultaneously, the first
      // callback to complete will close the other via closeConnectionsExceptKey. When
      // the second callback fires, its connection is already gone — bail out to avoid
      // closing the now-active connection and triggering an unwanted unregister.
      if (!this.connections[key]) {
        logger.info(`id ${this.id} key ${key} connection replaced before duplicate check completed, skipping`);
        return;
      }

      const { duplicated, oldDeviceId, newDeviceId } = data;

      logger.info(`id ${this.id} key ${key} isLoginDuplicated duplicate: ${duplicated}, ` +
        `oldDeviceId: ${oldDeviceId}, newDeviceId: ${newDeviceId}, ERR: ${err ? err.message : null }`);

      if (duplicated) {
        logger.info(`id ${this.id} key ${key} DUPLICATE_LOGIN device ${deviceId}` +
        ` connections ${Object.keys(this.connections).length}`);
        this.sendLoginDuplicatedMessageFromKeyWithDeviceId(duplicated, key, oldDeviceId, newDeviceId, undefined);
        Redis.setDeviceIdOfUser(this.id, deviceId);
      }

      this.periodicPing();

      debug(`id ${this.id} key ${key} REGISTERED device ${deviceId}` +
            ` connections ${Object.keys(this.connections).length}`);

      this.closeConnectionsExceptKey(key);
    });
  }

  public send(this: Client, data: Buffer) {
    Object.keys(this.connections).forEach((key) => {
      this.connections[key].send(data);
    });
  }

  // True iff this Client has at least one Connection whose underlying socket
  // is in WebSocket.OPEN state. Used by Server.isUserLive to distinguish a
  // genuinely connected user from one whose Client record is still around but
  // every transport has died — important for short-circuiting stale BUSY
  // rejections when the target's previous call wasn't torn down cleanly.
  public hasLiveConnection(this: Client): boolean {
    const keys = Object.keys(this.connections);
    for (const k of keys) {
      const conn = this.connections[k];
      if (conn && conn.isOpen()) { return true; }
    }
    return false;
  }

  public message(this: Client, message: IMessage, key0?: string) {
    Object.keys(this.connections).forEach((key) => {
      if (key0 && key0 === key) { return; }
      this.connections[key].message(message);
    });
  }

  public unregister(this: Client) {
    clearInterval(this.pingInterval);
    this.pingInterval = null;
    States.getGroupsWithActiveParticipant(this.id, (err, activeGroups) => {
      States.removeActiveParticipantFromAllGroups(this.id);
      States.releaseFloorOwnershipForUser(this.id);
      States.releasePrivateFloorOwnershipForUser(this.id);
      this.emit("unregister", this, activeGroups || []);
    });
    this.closeConnections();
  }

  private closeConnections(this: Client, key0?: string) {
    Object.keys(this.connections).forEach((key) => {
      logger.info(`id: ${this.id} closeConnections key: ${key}`);
      const connection = this.connections[key];
      this.unregisterConnection(connection);
    });
  }

  private closeConnectionsExceptKey(this: Client, key0: string) {
    Object.keys(this.connections).forEach((key) => {
      if (key0 === key) { return; }
      const connection = this.connections[key];
      this.unregisterConnection(connection);
    });
  }

  private unregisterConnection(this: Client, connection: Connection) {
    connection.removeListener("close", this.handleConnectionClose);
    connection.removeListener("message", this.handleConnectionMessage);
    connection.removeListener("pong", this.handleConnectionPong);
    connection.close();

    delete this.connections[connection.key];
    debug(`id ${this.id} key ${connection.key} UNREGISTERED device ${connection.deviceId}` +
          ` connections ${Object.keys(this.connections).length}`);
  }

  private periodicPing(this: Client) {
    if (this.pingInterval) { return; }
    const connections = Object.keys(this.connections).length;
    if (connections <= 0) {
      this.unregister();
      return;
    }

    this.pingInterval = setInterval(() => {
      this.ping();
    }, PING_INTERVAL);
  }

  private ping(this: Client) {
    Object.keys(this.connections).forEach((key) => {
      const connection = this.connections[key];
      if (!connection) { return; }
      const idleTime = Date.now() - connection.getLastSeenAt();
      if (idleTime > PING_TIMEOUT) {
        logger.info(`id ${this.id} key ${key} ping timeout after ${idleTime}ms, closing socket`);
        connection.closeDueToHeartbeatTimeout(idleTime);
        return;
      }
      connection.ping();
    });
  }

  private addToGroup(this: Client, groupId: numberOrString) {
    Redis.addUserToGroup(this.id, groupId, (err, succeed) => {
      Redis.getUsersInsideGroup(groupId, (err1, userIds) => {
        States.setUsersInsideGroup(groupId, userIds);
      });
    });
  }

  private removeFromGroup(this: Client, groupId: numberOrString) {
    Redis.removeUserFromGroup(this.id, groupId, (err, succeed) => {
      Redis.getUsersInsideGroup(groupId, (err1, userIds) => {
        States.setUsersInsideGroup(groupId, userIds);
      });
    });
  }

  private isLoginDuplicated(this: Client, deviceId: string, key: string,
                            callback: (err: Error, data: any) => void) {

    Redis.getDeviceIdOfUser(this.id, (err, deviceId1) => {
      const duplicated = !(deviceId && deviceId.length > 0 &&
                           deviceId1 && deviceId1 === deviceId);
      debug(`id ${this.id} isLoginDuplicated: ${duplicated}, deviceId: ${deviceId}`);
      return callback(err, { duplicated, oldDeviceId: deviceId1, newDeviceId: deviceId });
    });
  }

  private sendLoginDuplicatedMessageFromKeyWithDeviceId(this: Client, isDuplicate: boolean, key: string,
                                                        oldDeviceId: string, newDeviceId: string,
                                                        callback: () => void) {
    debug(`id ${this.id} key ${key} sendLoginDuplicatedWithDeviceId ${newDeviceId}`);
    const msg = {
      channelType: ChannelType.PRIVATE,
      fromId: 0,
      messageType: MessageType.LOGIN_DUPLICATED,
      payload: `userId ${this.id} has logged in from another deviceId ${newDeviceId}`,
      toId: this.id
    };
    this.message(msg, key);
  }

  // PRIVATE MESSAGE HANDLERS
  private handlePrivateMessage(this: Client, msg: IMessage) {
    if (msg.messageType === MessageType.DELIVERED) {
      return this.handleDeliveredMessage(msg);
    } else if (msg.messageType === MessageType.READ) {
      return this.handleReadMessage(msg);
    } else if (msg.messageType === MessageType.TEXT) {
      return this.handleTextMessage(msg);
    } else if (msg.messageType === MessageType.INTERACTIVE) {
      return this.handleTextMessage(msg);
    } else if (msg.messageType === MessageType.IMAGE) {
      return this.handleImageMessage(msg);
    } else if (msg.messageType === MessageType.START) {
      return this.handlePrivateStartMessage(msg);
    } else if (msg.messageType === MessageType.AUDIO) {
      return this.handlePrivateAudioMessage(msg);
    } else if (msg.messageType === MessageType.STOP) {
      return this.handleStopMessage(msg);
    } else if (msg.messageType === MessageType.CONNECTION_TEST) {
      return this.handleConnectionTest(msg);
    }
    debug(`id: ${this.id} handlePrivateMessage UNHANDLED ${JSON.stringify(msg)}`);
  }

  private handlePrivateAudioMessage(this: Client, msg: IMessage) {
    // Only broadcast audio if the sender owns the private-channel floor.
    States.isPrivateFloorOwner(msg.fromId, msg.toId, msg.fromId, (err, isOwner) => {
      if (err || !isOwner) {
        debug(`id ${this.id} ignoring AUDIO from non-owner ${msg.fromId} for private ${msg.toId}`);
        return;
      }

      Recorder.resume(msg);
      // Heartbeat: refresh the "Busy" session TTL in Redis for both participants while audio is flowing.
      States.refreshUserActiveCall(msg.fromId);
      States.refreshUserActiveCall(msg.toId);

      this.emit("message", msg, this);
    });
  }

  private handlePrivateStartMessage(this: Client, msg: IMessage) {
    const operationId = this.extractOperationId(msg);
    this.withOperationDedupe(operationId, msg, "START", () => this.handlePrivateStartMessageCore(msg));
  }

  private handlePrivateStartMessageCore(this: Client, msg: IMessage) {
    debug(`id ${this.id} handlePrivateStartMessage ${JSON.stringify(msg)}`);

    // Reject START messages to invalid/system targets. "00000", "0", and empty
    // are SDK heartbeat/echo addresses — allowing them sets stale call state that
    // blocks all future real calls for both the sender and the fake target.
    const toIdStr = (msg.toId || "").toString().replace(/^0+$/, "0");
    if (!msg.toId || toIdStr === "0" || toIdStr === "00000") {
      logger.info(`handlePrivateStartMessage: rejecting START from ${msg.fromId}` +
                  ` to invalid target "${msg.toId}" — ignoring`);
      return;
    }

    const newCallIsSos = this.parseIsSosCall(msg);
    const newCallIsInterrupt = this.parseIsInterruptCall(msg);

    // OFFLINE check: target has no client record at all (app killed, Doze, lost
    // connection without reconnect). Distinguish from BUSY so the caller can send
    // a wake-up push instead of just showing "busy". SOS/interrupt skip this
    // because their override paths handle stale state via isUserLive below.
    if (!newCallIsSos && !newCallIsInterrupt && !this.server.isUserConnected(msg.toId)) {
      logger.info(`handlePrivateStartMessage: target ${msg.toId} OFFLINE` +
                  ` — rejecting ${msg.fromId} with Offline so caller can wake via push`);
      this.message({
        channelType: msg.channelType, fromId: msg.fromId,
        messageType: MessageType.START_FAILED, payload: "Offline", toId: msg.toId
      });
      this.sendBusyEventText(msg, "Offline");
      return;
    }

    // Register in-flight START so a quick-release STOP can be buffered (same pattern
    // as pendingGroupStart/pendingGroupStop for GROUP calls).
    const privateStartStopKey = `${msg.fromId}_${msg.toId}`;
    if (this.pendingPrivateStart.has(privateStartStopKey)) {
      this.pendingPrivateStop.delete(privateStartStopKey);
    }
    this.pendingPrivateStart.set(privateStartStopKey, msg);
    setTimeout(() => {
      if (this.pendingPrivateStart.has(privateStartStopKey)) {
        logger.info(`handlePrivateStartMessage: TTL expiry — clearing stale pending START` +
                    ` for ${msg.fromId}→${msg.toId}`);
        this.pendingPrivateStart.delete(privateStartStopKey);
        this.pendingPrivateStop.delete(privateStartStopKey);
      }
    }, 15000);

    // 1. Check if the SENDER is already in a different active call.
    States.getCallDetailsForUser(msg.fromId, (err1, senderDetails) => {
      if (err1) {
        logger.info(`handlePrivateStartMessage: sender ${msg.fromId} lookup error: ${err1}`);
      }

      // 2. Target check — defined as a named closure so both the synchronous and
      //    asynchronous (floor-ownership) sender-stale paths can reach it without
      //    duplicating the entire block.
      const doTargetCheck = () => {
        States.getCallDetailsForUser(msg.toId, (err2, targetDetails) => {
          if (err2) {
            logger.info(`handlePrivateStartMessage: target ${msg.toId} lookup error: ${err2}`);
          }

          logger.info(`handlePrivateStartMessage check: target=${msg.toId} inCall=${targetDetails.inCall}` +
                      ` targetBusyWith=${targetDetails.targetId} sender=${msg.fromId}`);

          if (targetDetails.inCall) {
            // Busy WITH THE SENDER — allow (same-session heartbeat).
            if (targetDetails.targetId.toString() === msg.fromId.toString()) {
              logger.info(`handlePrivateStartMessage: continuing existing session` +
                          ` between ${msg.fromId} and ${msg.toId}`);
              this.proceedWithPrivateStart(msg, newCallIsSos, newCallIsInterrupt);
              return;
            }

            // Peer is disconnected — state is definitively stale.
            if (targetDetails.channelType !== 2 && !this.server.isUserConnected(targetDetails.targetId)) {
              logger.info(`handlePrivateStartMessage: clearing stale private call state for` +
                          ` target ${msg.toId} (was linked to disconnected peer ${targetDetails.targetId})`);
              States.clearUserPrivateCall(msg.toId);
              States.clearUserPrivateCall(targetDetails.targetId);
              this.proceedWithPrivateStart(msg, newCallIsSos, newCallIsInterrupt);
              return;
            }

            // Target itself has no live socket — its busy state is definitively
            // stale (you cannot be in a call without an OPEN transport).
            // Happens when a previous call ended uncleanly (force-kill, hard
            // network loss, router restart) and the Redis u.{target}.ac key is
            // still alive within its ~125s TTL. Clear and accept the call;
            // existing dedupe handles any in-flight stray STOP as a no-op.
            //
            // Carve-out: if the incoming call is SOS or warikomi AND the
            // stale-state's peer is still connected, defer to the existing
            // SOS/interrupt override path below so the peer receives the
            // CallEndedForAll / override notification (preserves SOS contract
            // for the connected peer rather than silently dropping its state).
            const peerStillConnected = targetDetails.channelType !== 2 &&
              this.server.isUserConnected(targetDetails.targetId);
            const deferToOverridePath = (newCallIsSos || newCallIsInterrupt) && peerStillConnected;
            if (!this.server.isUserLive(msg.toId) && !deferToOverridePath) {
              logger.info(`handlePrivateStartMessage: clearing stale call state for target ${msg.toId}` +
                          ` — target has no live socket (channelType=${targetDetails.channelType},` +
                          ` peer=${targetDetails.targetId})`);
              if (targetDetails.channelType === 1) {
                States.clearUserPrivateCall(msg.toId);
                States.clearUserPrivateCall(targetDetails.targetId);
              } else {
                States.removeActiveParticipantFromAllGroups(msg.toId);
                States.clearUserActiveCall(msg.toId);
              }
              this.proceedWithPrivateStart(msg, newCallIsSos, newCallIsInterrupt);
              return;
            }

            // Peer appears connected in a private call. We rely on the session TTL
            // rather than the floor lock so we do not drop the call during silence between turns.
            if (targetDetails.channelType === 1) {
              // Allow SOS or warikomi (interrupt) to override a busy target.
              // Normal calls are always rejected when target is in a non-SOS call.
              if ((!newCallIsSos && !newCallIsInterrupt) || targetDetails.isSos) {
                logger.info(
                  `handlePrivateStartMessage: target ${msg.toId} busy` +
                  ` (existingSos=${targetDetails.isSos} newSos=${newCallIsSos}` +
                  ` newInterrupt=${newCallIsInterrupt}) — rejecting ${msg.fromId}`
                );
                this.sendOverlapMissedCallText(msg, msg.toId);
                this.message({
                  channelType: msg.channelType, fromId: msg.fromId,
                  messageType: MessageType.START_FAILED, payload: "Busy", toId: msg.toId
                });
                this.sendBusyEventText(msg, "Busy");
                return;
              }
              logger.info(`handlePrivateStartMessage: SOS override — ending existing call for ${msg.toId}`);
              this.executeCallOverrideForUser(msg.toId, targetDetails, () => {
                this.proceedWithPrivateStart(msg, newCallIsSos, newCallIsInterrupt);
              });
              return;
            }

            // targetDetails.isSos can be stale if the user reconnected mid-SOS (isSos was
            // reset to false in reconnect handling). Use groupSosState as the authoritative
            // fallback so a private SOS can't eject a member from a group SOS call.
            const targetGroupInSos = targetDetails.isSos ||
              (targetDetails.channelType === 2 && States.isGroupSos(targetDetails.targetId));
            if ((!newCallIsSos && !newCallIsInterrupt) || targetGroupInSos) {
              // Reject: normal→any or sos→sos or interrupt→sos
              logger.info(`handlePrivateStartMessage: target ${msg.toId} busy (existingSos=${targetGroupInSos}` +
                          ` newSos=${newCallIsSos} newInterrupt=${newCallIsInterrupt}) — rejecting ${msg.fromId}`);
              this.sendOverlapMissedCallText(msg, msg.toId);
              this.message({
                channelType: msg.channelType,
                fromId: msg.fromId,
                messageType: MessageType.START_FAILED,
                payload: "Busy",
                toId: msg.toId
              });
              this.sendBusyEventText(msg, "Busy");
              return;
            }

            // SOS overrides a non-SOS group call: end the target's existing call first.
            logger.info(`handlePrivateStartMessage: SOS override — ending existing call for ${msg.toId}`);
            this.executeCallOverrideForUser(msg.toId, targetDetails, () => {
              this.proceedWithPrivateStart(msg, newCallIsSos, newCallIsInterrupt);
            });
            return;
          }

          this.proceedWithPrivateStart(msg, newCallIsSos, newCallIsInterrupt);
        });
      };

      if (!err1 && senderDetails.inCall) {
        // If the sender is busy with someone else, they can't start a new call.
        if (senderDetails.targetId.toString() !== msg.toId.toString()) {
          // Peer is disconnected — state is definitively stale.
          if (senderDetails.channelType !== 2 && !this.server.isUserConnected(senderDetails.targetId)) {
            logger.info(`handlePrivateStartMessage: clearing stale private call state for` +
                        ` ${msg.fromId} (was linked to disconnected peer ${senderDetails.targetId})`);
            States.clearUserPrivateCall(msg.fromId);
            States.clearUserPrivateCall(senderDetails.targetId);
            doTargetCheck();
            return;
          }
          // If the sender themselves is initiating a new private call, they are leaving
          // their previous call client-side. We clear the previous state and proceed.
          if (senderDetails.channelType === 1) {
            logger.info(`handlePrivateStartMessage: sender ${msg.fromId} initiating new call to ${msg.toId}` +
                        ` — clearing previous private call state with ${senderDetails.targetId}`);
            States.clearUserPrivateCall(msg.fromId);
            States.clearUserPrivateCall(senderDetails.targetId);
            doTargetCheck();
            return;
          }
          // Sender is in a group call but initiating a new private call — allow them to leave the group.
          if (senderDetails.channelType === 2) {
            logger.info(`handlePrivateStartMessage: sender ${msg.fromId} initiating new call to ${msg.toId}` +
                        ` — leaving group ${senderDetails.targetId} to start private call`);
            States.removeUserFromActiveCallGroup(msg.fromId, senderDetails.targetId, () => {
              States.clearUserActiveCall(msg.fromId);
              doTargetCheck();
            });
            return;
          }
          // Group-type busy — reject immediately (floor check not applicable for group floors).
          logger.info(`handlePrivateStartMessage: sender ${msg.fromId} is busy with` +
                      ` ${senderDetails.targetId} — rejecting call to ${msg.toId}`);
          this.message({
            channelType: msg.channelType,
            fromId: msg.fromId,
            messageType: MessageType.START_FAILED,
            payload: "Busy",
            toId: msg.toId
          });
          this.sendBusyEventText(msg, "Busy");
          return;
        }
      }

      doTargetCheck();
    });
  }

  private proceedWithPrivateStart(
    this: Client,
    msg: IMessage,
    isSos: boolean,
    isInterrupt: boolean = false,
    allowInterruptRetry: boolean = true
  ) {
    // Acquire the private-channel floor before allowing the call to proceed.
    // This is synchronous so it is atomic within a single server process:
    // if both users press simultaneously, only the first START wins.
    States.acquirePrivateFloor(msg.fromId, msg.toId, msg.fromId, (err, acquired, currentOwner) => {
      if (!acquired) {
        const currentOwnerStr = (currentOwner || "").toString();
        if (isInterrupt && allowInterruptRetry && currentOwnerStr && currentOwnerStr !== msg.fromId.toString()) {
          logger.info(`handlePrivateStartMessage: interrupt START from ${msg.fromId}` +
                      ` preempting private floor owner ${currentOwnerStr} for ${msg.fromId}↔${msg.toId}`);
          this.forceStopCurrentPrivateFloorOwner(msg.fromId, msg.toId, currentOwnerStr, () => {
            this.proceedWithPrivateStart(msg, isSos, false, false);
          });
          return;
        }
        // Cluster-safe orphan check: verify the floor owner is still in a private call
        // with one of the parties. getCallDetailsForUser checks in-memory then Redis,
        // so it works correctly across all worker processes.
        if (currentOwnerStr && currentOwnerStr !== "0") {
          return States.getCallDetailsForUser(currentOwnerStr, (detailsErr, ownerDetails) => {
            const fromStr = msg.fromId.toString();
            const toStr   = msg.toId.toString();
            const floorIsOrphaned = detailsErr || !ownerDetails.inCall ||
              ownerDetails.channelType !== 1 ||
              (ownerDetails.targetId !== fromStr && ownerDetails.targetId !== toStr);
            if (floorIsOrphaned) {
              logger.info(`proceedWithPrivateStart: private floor owner ${currentOwnerStr} has no` +
                          ` matching call state — force-releasing orphaned floor for ${msg.fromId}↔${msg.toId}`);
              return States.releasePrivateFloor(msg.fromId, msg.toId, currentOwnerStr, () => {
                this.proceedWithPrivateStart(msg, isSos, isInterrupt, false);
              });
            }
            logger.info(`handlePrivateStartMessage: floor busy for ${msg.fromId}→${msg.toId},` +
                        ` owner: ${currentOwner} — sending START_FAILED`);
            this.message({
              channelType: msg.channelType,
              fromId: msg.fromId,
              messageType: MessageType.START_FAILED,
              payload: "Busy",
              toId: msg.toId
            });
          });
        }
        logger.info(`handlePrivateStartMessage: floor busy for ${msg.fromId}→${msg.toId},` +
                    ` owner: ${currentOwner} — sending START_FAILED`);
        this.message({
          channelType: msg.channelType,
          fromId: msg.fromId,
          messageType: MessageType.START_FAILED,
          payload: "Busy",
          toId: msg.toId
        });
        return;
      }
      // Mark both users as in an active private call globally in Redis.
      States.setUserPrivateCall(msg.fromId, msg.toId, isSos);
      States.setUserPrivateCall(msg.toId, msg.fromId, isSos);

      Recorder.start(msg);
      this.acknowledgePrivateStartMessage(msg);
      States.setCurrentMessageOfUser(msg.fromId, msg);
      this.emit("message", msg, this);

      // Clear in-flight marker and replay any STOP that arrived before floor was ready.
      // Delay 400 ms so the receiver processes START before STOP arrives (same as GROUP).
      const privateKey = `${msg.fromId}_${msg.toId}`;
      this.pendingPrivateStart.delete(privateKey);
      const bufferedPrivateStop = this.pendingPrivateStop.get(privateKey);
      if (bufferedPrivateStop) {
        this.pendingPrivateStop.delete(privateKey);
        logger.info(`proceedWithPrivateStart: processing buffered STOP for` +
                    ` ${msg.fromId}→${msg.toId} (quick tap-and-release, 400ms delay)`);
        setTimeout(() => this.finishStopMessage(bufferedPrivateStop), 400);
      }
    });
  }

  /**
   * Returns true if either party currently holds the private floor in Redis.
   * Used to distinguish a genuinely active private call from stale in-call state
   * left behind when a call ended without a clean STOP reaching the router.
   */
  private isPrivateCallFloorHeld(
    this: Client,
    userId1: numberOrString,
    userId2: numberOrString,
    callback: (held: boolean) => void
  ): void {
    States.isPrivateFloorOwner(userId1, userId2, userId1, (e1, u1Owns) => {
      if (u1Owns) { return callback(true); }
      States.isPrivateFloorOwner(userId1, userId2, userId2, (e2, u2Owns) => callback(u2Owns));
    });
  }

  /** General override logic to end a user's current call session (private or group). */
  private executeCallOverrideForUser(
    userId: numberOrString,
    currentCall: { channelType: number; targetId: numberOrString; isSos: boolean },
    callback: () => void
  ) {
    if (currentCall.channelType === 1) { // PRIVATE
      States.clearUserPrivateCall(userId);
      States.clearUserPrivateCall(currentCall.targetId);
      States.releasePrivateFloorOwnershipForUser(userId);
      // Notify BOTH sides. The DropCall delivered TO userId carries isSosOverride=true so
      // the mobile keeps its service alive and can receive the SOS START ~800ms later.
      this.sendDropCallToUser(userId, currentCall.targetId, 1, 0, undefined, false);
      this.sendDropCallToUser(currentCall.targetId, userId, 1, 0, undefined, true);
      // IMPORTANT: must call callback() here so that Q.all(overrides) resolves
      // and proceed() forwards the SOS START to the group.  Without this the
      // SOS call is silently swallowed and the group floor leaks until TTL.
      callback();
    } else { // GROUP
      const groupId = currentCall.targetId;
      // Clear the user's Redis active-call key immediately so subsequent overlap
      // checks don't see them as still busy in the group they are being ejected from.
      States.clearUserActiveCall(userId);
      // Release the group floor if this user holds it. Without this the floor stays
      // locked until TTL, blocking the remaining group members from speaking.
      States.releaseFloorOfGroup(groupId, userId);
      States.removeUserFromActiveCallGroup(userId, groupId, (err, count) => {
        // 1. Targeted DropCall to the overridden user with isSosOverride=true so
        //    their service stays alive for the incoming SOS START.
        this.sendDropCallToUser("System", groupId, 2, count, userId, true);

        // Notify remaining group members of the reduced participant count.
        States.getActiveParticipantsOfGroup(groupId, (apErr, remaining) => {
          (remaining || []).forEach((pid) => {
            if (pid.toString() !== userId.toString()) {
              this.sendDropCallToUser(userId + "", groupId, 2, count, pid, false);
            }
          });
        });

        callback();
      });
    }
  }

  /** Sends a DropCall text message to the peer or group. */
  private sendDropCallToUser(
    fromId: numberOrString,
    toId: numberOrString,
    channelType: number,
    membersInCall: number = 0,
    deliveryId?: numberOrString,
    isSosOverride: boolean = false
  ) {
    const dropMsg = {
      channelType,
      fromId,
      messageId: JSON.stringify({
        callId: toId.toString(),
        errorType: isSosOverride ? "SosOverride" : "",
        lang: "",
        membersInCall,
        textMessageType: "DropCall",
        translate: false
      }),
      messageType: MessageType.TEXT,
      payload: JSON.stringify({
        message_id: "DropCall",
        text: "DropCall"
      }),
      toId
    };
    if (deliveryId) {
      this.server.sendMessageToUser(dropMsg, deliveryId);
    } else {
      if (channelType === 1) {
        this.emit("message", dropMsg, this); // Broadcast to peer via Server
      } else {
        this.server.sendMessageToGroup(dropMsg);
      }
    }
  }

  /** Sends a BusyEvent text message back to the caller so their UI updates correctly.
   *
   * Private calls: toId = caller so the server routes the message to them.
   *   Mobile filter: tempCallId = fromID (target) = currentCallId ✓
   *
   * Group calls: toId = groupId (NOT callerId) so that tempCallId = toID = groupId = currentCallId.
   *   The message is delivered only to the caller via sendMessageToUser, not broadcast to group.
   */
  private sendBusyEventText(this: Client, msg: IMessage, errorType: string) {
    const isGroup = msg.channelType === 2;
    // For group: callId in messageId must be the group ID so the mobile isSameCall check passes.
    // For private: callId is the caller's ID (existing behaviour).
    const callIdInMeta = isGroup ? msg.toId.toString() : msg.fromId.toString();
    // For group: toId must be the group ID so mobile's tempCallId (= toID) matches currentCallId.
    // For private: toId is the caller so the server routes the message to them.
    const toId = isGroup ? msg.toId : msg.fromId;

    const busyMsg = {
      channelType: msg.channelType,
      fromId: msg.toId,   // "from" the target or group that is busy
      messageId: JSON.stringify({
        callId: callIdInMeta,
        channelType: msg.channelType,
        errorType,
        lang: "",
        membersInCall: 0,
        senderPtt: msg.toId.toString(),
        textMessageType: "BusyEvent",
        translate: false
      }),
      messageType: MessageType.TEXT,
      payload: JSON.stringify({
        message_id: "BusyEvent",
        text: "BusyEvent"
      }),
      toId
    };

    if (isGroup) {
      // Deliver only to the caller — do not broadcast to all group members.
      this.server.sendMessageToUser(busyMsg, msg.fromId);
    } else {
      this.emit("message", busyMsg, this);
    }
  }

  private getOverlapMissedCallKey(this: Client, msg: IMessage, recipientId: numberOrString): string {
    if (msg.channelType === ChannelType.GROUP) {
      return `${msg.channelType}:${msg.toId}:${recipientId}`;
    }
    return `${msg.channelType}:${msg.fromId}:${msg.toId}:${recipientId}`;
  }

  private reserveOverlapMissedCallKey(this: Client, key: string): boolean {
    const isNew = !this.overlapMissedCallKeys.has(key);
    this.overlapMissedCallKeys.add(key);

    const existingTimer = this.overlapMissedCallKeyTimers.get(key);
    if (existingTimer) {
      clearTimeout(existingTimer);
    }

    const timer = setTimeout(() => {
      this.overlapMissedCallKeys.delete(key);
      this.overlapMissedCallKeyTimers.delete(key);
    }, OVERLAP_MISSED_CALL_DEDUPE_TTL_MS);
    this.overlapMissedCallKeyTimers.set(key, timer);

    return isNew;
  }

  private clearOverlapMissedCallKeysByPrefix(this: Client, prefix: string): void {
    Array.from(this.overlapMissedCallKeys)
      .filter((key) => key.startsWith(prefix))
      .forEach((key) => {
        this.overlapMissedCallKeys.delete(key);
        const timer = this.overlapMissedCallKeyTimers.get(key);
        if (timer) {
          clearTimeout(timer);
          this.overlapMissedCallKeyTimers.delete(key);
        }
      });
  }

  private clearOverlapMissedCallKeysForTextMessage(this: Client, msg: IMessage, meta: ITextMessageMeta): void {
    if (!meta || !meta.textMessageType) { return; }
    if (msg.channelType === ChannelType.GROUP && meta.textMessageType === "CallEndedForAll") {
      this.clearOverlapMissedCallKeysByPrefix(`${ChannelType.GROUP}:${msg.toId}:`);
      return;
    }
    if (msg.channelType === ChannelType.PRIVATE &&
        (meta.textMessageType === "EndCall" || meta.textMessageType === "DropCall")) {
      this.clearOverlapMissedCallKeysByPrefix(`${ChannelType.PRIVATE}:${msg.fromId}:${msg.toId}:`);
      this.clearOverlapMissedCallKeysByPrefix(`${ChannelType.PRIVATE}:${msg.toId}:${msg.fromId}:`);
    }
  }

  /** Sends an OfflineMembers TEXT to the group-call caller with the list of PTT
   *  numbers that were not connected. Caller uses this list to send wake-up pushes. */
  private sendOfflineMembersText(
    this: Client,
    msg: IMessage,
    offlinePttNos: numberOrString[]
  ): void {
    const pttNos = offlinePttNos.map((id) => id + "");
    const offlineMsg = {
      channelType: msg.channelType,
      fromId: msg.toId,
      messageId: JSON.stringify({
        callId: msg.toId.toString(),
        errorType: "Offline",
        lang: "",
        membersInCall: 0,
        offlinePttNos: pttNos,
        textMessageType: "OfflineMembers",
        translate: false
      }),
      messageType: MessageType.TEXT,
      payload: JSON.stringify({
        message_id: "OfflineMembers",
        text: JSON.stringify({ callId: msg.toId.toString(), offlinePttNos: pttNos })
      }),
      toId: msg.fromId
    };
    logger.info(`sendOfflineMembersText: notifying caller ${msg.fromId}` +
                ` group ${msg.toId} offlineCount=${pttNos.length}`);
    this.server.sendMessageToUser(offlineMsg, msg.fromId);
  }

  private sendOverlapMissedCallText(this: Client, msg: IMessage, recipientId: numberOrString): void {
    const key = this.getOverlapMissedCallKey(msg, recipientId);
    if (!this.reserveOverlapMissedCallKey(key)) {
      logger.info(`sendOverlapMissedCallText: duplicate skipped key=${key}`);
      return;
    }

    const isGroup = msg.channelType === ChannelType.GROUP;
    const callId = isGroup ? msg.toId.toString() : msg.fromId.toString();
    const missedMsg = {
      channelType: msg.channelType,
      fromId: msg.fromId,
      messageId: JSON.stringify({
        callId,
        errorType: "OverlapCall",
        lang: "",
        membersInCall: 0,
        textMessageType: "OverlapMissedCall",
        translate: false
      }),
      messageType: MessageType.TEXT,
      payload: JSON.stringify({
        message_id: key,
        text: JSON.stringify({ callId, dedupeKey: key })
      }),
      toId: isGroup ? msg.toId : recipientId
    };

    logger.info(`sendOverlapMissedCallText: notifying busy recipient ${recipientId}` +
                ` for callId=${callId} key=${key}`);
    this.server.sendMessageToUser(missedMsg, recipientId);
  }

  /** Parses the START message payload when the caller sends JSON metadata. */
  private parseStartPayload(this: Client, msg: IMessage): any | null {
    try {
      return JSON.parse(msg.payload.toString());
    } catch {
      return null;
    }
  }

  /** Parses the isSosCall flag from the START message payload. */
  private parseIsSosCall(this: Client, msg: IMessage): boolean {
    const data = this.parseStartPayload(msg);
    return !!data && (data.isSosCall === true || data.onlyConnectCallOnLongPress === true);
  }

  /** Parses the isInterrupt flag from the START message payload. */
  private parseIsInterruptCall(this: Client, msg: IMessage): boolean {
    const data = this.parseStartPayload(msg);
    return !!data && data.isInterrupt === true;
  }

  private acknowledgePrivateStartMessage(this: Client, msg: IMessage) {
    const payload = msg.payload || "Acknowledged";
    this.server.sendMessageToUser({
      ...msg,
      channelType: msg.channelType,
      messageType: MessageType.START_ACK,
      payload
    }, msg.fromId);
  }

  // PRIVATE & GROUP (USED BY BOTH) MESSAGE HANDLERS

  private handleStopMessage(this: Client, msg: IMessage) {
    const operationId = this.extractOperationId(msg);
    this.withOperationDedupe(operationId, msg, "STOP", () => this.handleStopMessageCore(msg));
  }

  private handleStopMessageCore(this: Client, msg: IMessage) {
    logger.info(`handleStopMessage id ${msg.fromId} to ${msg.toId} messageType ${msg.messageType}`);

    if (msg.channelType === ChannelType.GROUP) {
      return States.isFloorOwnerOfGroup(msg.toId, msg.fromId, (ownerErr, isOwner) => {
        if (ownerErr) {
          debug(`id ${this.id} handleStopMessage owner check err ${ownerErr}`);
          return;
        }
        if (!isOwner) {
          // If a START for this user/group is still being processed (async floor
          // acquisition), buffer this STOP so it gets replayed once START finishes.
          // This handles quick tap-and-release where STOP arrives before the floor
          // state is committed (race condition on short talk durations).
          const startStopKey = `${msg.fromId}_${msg.toId}`;
          if (this.pendingGroupStart.has(startStopKey)) {
            logger.info(`handleStopMessage: buffering STOP for user ${msg.fromId}` +
                        ` group ${msg.toId} — START still in-flight`);
            this.pendingGroupStop.set(startStopKey, msg);
            return;
          }
          debug(`id ${this.id} ignoring STOP from non-owner ${msg.fromId} for group ${msg.toId}`);
          return;
        }
        return this.finishStopMessage(msg);
      });
    }

    // Private: verify the sender owns the floor before processing STOP.
    // Without this check a non-owner STOP would broadcast a spurious incomingStopTalked
    // to the other user and corrupt their state.
    return States.isPrivateFloorOwner(msg.fromId, msg.toId, msg.fromId, (ownerErr, isOwner) => {
      if (ownerErr) {
        debug(`id ${this.id} handleStopMessage private owner check err ${ownerErr}`);
        return;
      }
      if (!isOwner) {
        // If a START for this user is still being processed (async floor acquisition),
        // buffer this STOP so it gets replayed once START finishes — mirrors the
        // pendingGroupStop buffer used for GROUP quick tap-and-release.
        const privateKey = `${msg.fromId}_${msg.toId}`;
        if (this.pendingPrivateStart.has(privateKey)) {
          logger.info(`handleStopMessage: buffering STOP for private ${msg.fromId}→${msg.toId}` +
                      ` — START still in-flight`);
          this.pendingPrivateStop.set(privateKey, msg);
          return;
        }
        debug(`id ${this.id} ignoring STOP from non-owner ${msg.fromId} for private ${msg.toId}`);
        return;
      }
      // Note: We don't clearUserPrivateCall here anymore. Session persistency relies on Redis TTL
      // to bridge the "hang time" gap between PTT turns.
      return this.finishStopMessage(msg);
    });
  }

  private finishStopMessage(this: Client, msg: IMessage, callback?: () => void) {
    logger.info(`finishStopMessage id ${msg.fromId} to ${msg.toId} messageType ${msg.messageType}`);

    Recorder.stop(msg, (err, messageId, duration) => {
      setTimeout(() => {
        this.acknowledgeStopMessage(msg, messageId);

        const payload = JSON.stringify({
          duration,
          message_id: messageId
        });

        const msg1 = {
          ...msg,
          payload
        };

        this.emit("message", msg1, this);

        States.removeCurrentMessageOfUser(msg.fromId);
        if (msg.channelType === ChannelType.GROUP) {
          States.releaseFloorOfGroup(msg.toId, msg.fromId, () => {
            States.clearGroupFloorRecipients(msg.toId, msg.fromId);
            States.removeCurrentMessageOfGroup(msg.toId);
            if (callback) { callback(); }
          });
        } else {
          // Private channel: release the floor so the other user can speak next.
          // Do NOT clear userPrivateCallState here — session persistence between PTT
          // turns relies on the in-memory state being intact for the fast-path check
          // in getCallDetailsForUser.  Clearing it on every STOP forces two extra
          // Redis round-trips per PTT START (for sender + target), which adds
          // noticeable latency on high-RTT Redis deployments.
          // State is cleared properly when: (a) EndCall text is received, or
          // (b) either user disconnects (handleConnectionClose sends EndCall to peer).
          States.releasePrivateFloor(msg.fromId, msg.toId, msg.fromId);
          if (callback) { callback(); }
        }
        debug(`id ${this.id} Done removing current message from states`);
        States.getBusyStateOfGroup(msg.toId, (err1, busy) => {
          debug(`id ${this.id} Checking busy state after removed: ${JSON.stringify(busy)}`);
        });
      }, 100);
    });
  }

  private acknowledgeStopMessage(this: Client, msg: IMessage, messageId: string) {
    debug(`id ${this.id} Response STOP_ACK`);
    this.server.sendMessageToUser({
      ...msg,
      messageId,
      messageType: MessageType.STOP_ACK,
      payload: "Acknowledged"
    }, msg.fromId);
  }

  private handleDeliveredMessage(this: Client, msg: IMessage) {
    this.emit("message", msg, this);

    let messages = [];
    try {
      messages = JSON.parse(msg.payload as string);
    } catch (exception) {
      debug(`id ${this.id} handleDeliveredMessage JSON.parse ERR ${exception} ${JSON.stringify(msg)}`);
      if (typeof msg.payload === "string") {
        messages = [msg.payload];
      }
    }
  }

  private handleReadMessage(this: Client, msg: IMessage) {
      this.emit("message", msg, this);
  }

  private handleImageMessage(this: Client, msg: IMessage) {
    Recorder.save(msg, (err, messageId) => {
      if (err) { logger.error(`Failed to save image at recorder. err: ${err}`); }
      States.getUsersInsideGroup(msg.toId, (err1, userIds1) => {
        const isSenderInGroup = userIds1
          ? userIds1.map((u) => u.toString()).includes(msg.fromId.toString())
          : false;
        if (!isSenderInGroup) {
          this.send27ToMe(msg);
        }
      });
    });
  }

  private handleTextMessage(this: Client, msg: IMessage) {
    // Clear private-call state synchronously BEFORE Recorder.save's async file I/O.
    // Doing this inside the save callback causes a race: if the file flush takes longer
    // than the ~280ms gap before the next group START, 0011/0013 appear busy to the
    // group call's member check even though EndCall has already been received.
    const earlyMeta = this.parseTextMessageMeta(msg);
    // The mobile app sends "DropCall" (not "EndCall") when a user ends a private call.
    // "EndCall" is sent by the router itself when forcibly terminating a call.
    // Both must clear the private-call state so the peer is no longer seen as busy.
    if (earlyMeta && msg.channelType === 1 &&
        (earlyMeta.textMessageType === "EndCall" || earlyMeta.textMessageType === "DropCall")) {
      States.clearUserPrivateCall(msg.fromId);
      States.clearUserPrivateCall(msg.toId);
    }
    if (earlyMeta) {
      this.clearOverlapMissedCallKeysForTextMessage(msg, earlyMeta);
    }

    Recorder.save(msg, (err, messageId) => {
      const meta = this.parseTextMessageMeta(msg);

      this.applyAuthoritativeParticipantCount(msg, meta, () => {
      this.acknowledgeTextMessage(msg, messageId);

      let text = msg.payload.toString();
      try {
        if (msg.messageType === MessageType.INTERACTIVE) {
          text = JSON.stringify(msg.payload);
        } else {
          const json = JSON.parse(msg.payload.toString());
          text = json.text;
        }
      } catch (exception) {
        debug(`id ${this.id} JSON.parse TEXT EXCEPTION ${exception}`);
        text = msg.payload.toString();
      }

      const payload = JSON.stringify({
        message_id: messageId,
        text
      });
      const msg1 = {
        ...msg,
        payload
      };

      if (msg.channelType === ChannelType.GROUP) {
        States.getUsersInsideGroup(msg.toId, (err1, userIds1) => {
          const isSenderInGroup = userIds1
            ? userIds1.map((u) => u.toString()).includes(msg.fromId.toString())
            : false;
          if (!isSenderInGroup) {
            this.send27ToMe(msg);
          }
        });
      }

      this.emit("message", msg1, this);
      });
    });
  }

  private acknowledgeTextMessage(this: Client, msg: IMessage, messageId: string) {
    this.message({
      channelType: msg.channelType,
      fromId: msg.fromId,
      messageId,
      messageType: (msg.messageType === MessageType.INTERACTIVE ?
                    MessageType.INTERACTIVE_ACK : MessageType.TEXT_ACK),
      payload: "Acknowledged",
      toId: msg.toId
    });
  }

  private parseTextMessageMeta(this: Client, msg: IMessage): ITextMessageMeta | null {
    if (!msg.messageId || typeof msg.messageId !== "string") { return null; }
    try {
      return JSON.parse(msg.messageId) as ITextMessageMeta;
    } catch (e) {
      debug(`id ${this.id} parseTextMessageMeta error ${e}`);
      return null;
    }
  }

  private applyAuthoritativeParticipantCount(
    this: Client,
    msg: IMessage,
    meta: ITextMessageMeta | null,
    callback: () => void
  ) {
    if (msg.channelType !== ChannelType.GROUP || !meta || !meta.textMessageType) {
      callback();
      return;
    }

    const groupId = msg.toId;
    const senderId = msg.fromId;

    switch (meta.textMessageType) {
      case "JoinAcknowledgement":
        const isGroupSos = States.isGroupSos(groupId);
        States.addUserToActiveCallGroup(senderId, groupId, isGroupSos, (err2, count) => {
          meta.membersInCall = count;
          msg.messageId = JSON.stringify(meta);
          callback();
        });
        return;
      case "DropCall":
        States.removeUserFromActiveCallGroup(senderId, groupId, (err, count) => {
          // Clear the user's active call state so they're no longer marked as busy with this group
          States.clearUserActiveCall(senderId);
          meta.membersInCall = count;
          msg.messageId = JSON.stringify(meta);
          // If this was the last participant, clean up the entire group state
          // to prevent stale state when clients don't send CallEndedForAll
          if (count === 0) {
            logger.info(`handleTextMessage DropCall: last participant left group ${groupId}` +
                        ` — cleaning up group call state`);
            States.clearActiveCallGroup(groupId, () => {
              callback();
            });
          } else {
            callback();
          }
        });
        return;
      case "CallEndedForAll":
        // Get all users in the group before clearing
        States.getUsersInsideGroup(groupId, (err, userIds) => {
          if (!err && userIds && userIds.length > 0) {
            // Clear ALL call state for every group member (production-ready cleanup)
            userIds.forEach((userId) => {
              // 1. Clear Redis active call entry
              States.clearUserActiveCall(userId);
              // 2. Clear private call state if exists
              States.clearUserPrivateCall(userId);
              // 3. Remove from all active call groups
              States.removeActiveParticipantFromAllGroups(userId);
              // 4. Release any floor ownership
              States.releaseFloorOwnershipForUser(userId);
              States.releasePrivateFloorOwnershipForUser(userId);
            });
          }

          // 5. Clear the group call state
          States.clearActiveCallGroup(groupId, () => {
            meta.membersInCall = 0;
            msg.messageId = JSON.stringify(meta);
            callback();
          });
        });
        return;
      default:
        callback();
        return;
    }
  }

  // GROUP MESSAGE HANDLERS

  private handleGroupMessage(this: Client, msg: IMessage) {
    // tslint:disable-next-line:max-line-length
    debug(`id ${this.id} handleGroupMessage => channelType: ${msg.channelType}, messageType: ${msg.messageType}, from: ${msg.fromId}, to: ${msg.toId}`);
    switch (msg.messageType) {
      case MessageType.TEXT:
        this.handleTextMessage(msg);
        break;
      case MessageType.INTERACTIVE:
        this.handleTextMessage(msg);
        break;
      case MessageType.IMAGE:
        this.handleImageMessage(msg);
        break;
      case MessageType.START:
        this.handleGroupStartMessage(msg);
        break;
      case MessageType.AUDIO:
        this.handleGroupAudioMessage(msg);
        break;
      case MessageType.STOP:
        this.handleStopMessage(msg);
        break;
      case MessageType.USER_ADD:
        this.addToGroup(msg.toId);
        break;
      case MessageType.USER_REMOVE:
        this.removeFromGroup(msg.toId);
        break;
      case MessageType.USER_REMOVE_ALL:
        this.emit("message", msg, this);
        break;
      case MessageType.DELIVERED:
        break;
      case MessageType.READ:
        break;
      case MessageType.CONNECTION:
        this.emit("message", msg, this);
        break;
      case MessageType.CONNECTION_TEST:
        this.handleConnectionTest(msg);
        break;
      default:
        debug(`id: ${this.id} handleGroupMessage: UNHANDLED: ${JSON.stringify(msg)}`);
        break;
    }
  }

  private handleGroupAudioMessage(this: Client, msg: IMessage) {
    logger.info(`handleGroupAudioMessage id ${msg.fromId} to ${msg.toId} messageType ${msg.messageType}`);
    // Snapshot recipients at owner-check time so we detect floor transfers that happen
    // during the async Recorder.resume I/O before routing the packet.
    States.isFloorOwnerOfGroup(msg.toId, msg.fromId, (ownerErr, isOwner) => {
      if (ownerErr) {
        debug(`id: ${this.id} handleGroupAudioMessage owner check err: ${ownerErr}`);
        return;
      }
      if (!isOwner) {
        debug(`id: ${this.id} dropping group AUDIO from non-owner ${msg.fromId} for group ${msg.toId}`);
        return;
      }
      // Capture the recipient list at the time ownership is confirmed.
      // If the floor transfers while Recorder.resume is in-flight, getGroupFloorRecipients
      // will return undefined for the old owner, which we use to drop the stale packet.
      const recipientsSnapshot = States.getGroupFloorRecipients(msg.toId, msg.fromId);
      States.refreshFloorOfGroup(msg.toId, msg.fromId);
      Recorder.resume(msg, (err, messageId, duration) => {
        if (err) { debug(`id: ${this.id} recorder.resume: err: ${err} messageId: ${messageId}` +
                         ` duration: ${duration}`); }

        // Re-check ownership after async I/O to guard against mid-callback floor transfer.
        const recipientsNow = States.getGroupFloorRecipients(msg.toId, msg.fromId);
        // If the snapshot had recipients but now the owner has changed (undefined returned
        // for old owner), drop this stale audio packet.
        if (recipientsSnapshot !== undefined && recipientsNow === undefined) {
          debug(`id: ${this.id} dropping AUDIO from ${msg.fromId} — floor transferred during I/O`);
          return;
        }

        // Use the floor-session recipient list when available (overlap-aware subset delivery).
        // undefined → normal call, broadcast to everyone in the group.
        // [] (empty) → all members were busy; drop this audio packet.
        // [...ids]  → partial availability; deliver only to the members who received START.
        const recipients = recipientsNow;
        if (recipients !== undefined) {
          if (recipients.length === 0) {
            debug(`id: ${this.id} dropping AUDIO from ${msg.fromId} — group ${msg.toId} recipients empty (all-busy)`);
            return;
          }
          this.server.sendMessageToGroupSubset(msg, recipients);
        } else {
          this.emit("message", msg, this);
        }
      });
    });
  }

  private handleGroupStartMessage(this: Client, msg: IMessage) {
    const operationId = this.extractOperationId(msg);
    this.withOperationDedupe(operationId, msg, "START", () => this.handleGroupStartMessageCore(msg));
  }

  private forceStopCurrentGroupFloorOwner(
    this: Client,
    groupId: numberOrString,
    floorOwnerId: numberOrString,
    callback: () => void
  ) {
    logger.info(`forceStopCurrentGroupFloorOwner: stopping owner ${floorOwnerId} in group ${groupId}`);
    this.finishStopMessage({
      channelType: ChannelType.GROUP,
      fromId: floorOwnerId,
      messageType: MessageType.STOP,
      payload: "Interrupted",
      toId: groupId
    }, callback);
  }

  private forceStopCurrentPrivateFloorOwner(
    this: Client,
    userId1: numberOrString,
    userId2: numberOrString,
    floorOwnerId: numberOrString,
    callback: () => void
  ) {
    const otherParticipantId =
      floorOwnerId.toString() === userId1.toString() ? userId2 : userId1;
    logger.info(`forceStopCurrentPrivateFloorOwner: stopping owner ${floorOwnerId}` +
                ` for private session ${userId1}↔${userId2}`);
    this.finishStopMessage({
      channelType: ChannelType.PRIVATE,
      fromId: floorOwnerId,
      messageType: MessageType.STOP,
      payload: "Interrupted",
      toId: otherParticipantId
    }, callback);
  }

  private handleGroupStartMessageCore(this: Client, msg: IMessage) {
    // Register the in-flight START so a quick-release STOP can be buffered.
    const startStopKey = `${msg.fromId}_${msg.toId}`;
    // If a previous START for this key never cleaned up (e.g. floor acquisition hung),
    // discard the stale buffered STOP so it cannot be replayed into this new session.
    if (this.pendingGroupStart.has(startStopKey)) {
      logger.info(`handleGroupStartMessage: new START arrived while previous still in-flight` +
                  ` for user ${msg.fromId} group ${msg.toId} — clearing stale STOP buffer`);
      this.pendingGroupStop.delete(startStopKey);
    }
    this.pendingGroupStart.set(startStopKey, msg);
    // Safety: if floor acquisition hangs beyond 15 s, clear both maps to prevent
    // a buffered STOP from a dead session being replayed into a future one.
    setTimeout(() => {
      if (this.pendingGroupStart.has(startStopKey)) {
        logger.info(`handleGroupStartMessage: TTL expiry — clearing stale pending START` +
                    ` for user ${msg.fromId} group ${msg.toId}`);
        this.pendingGroupStart.delete(startStopKey);
        this.pendingGroupStop.delete(startStopKey);
      }
    }, 15000);
    const isSos = this.parseIsSosCall(msg);
    const isInterrupt = this.parseIsInterruptCall(msg);
    const senderIdStr = msg.fromId.toString();

    // ── Step 1: Check sender membership + member availability BEFORE acquiring the floor.
    // This avoids sending START_ACK and then immediately sending BusyEvent when all
    // members are already occupied — which caused a visible "flash" in the caller's UI.
    States.getUsersInsideGroup(msg.toId, (err1, userIds1) => {
      debug(`handleGroupStartMessage - getUsersInsideGroup groupId: ${msg.toId} users: ${JSON.stringify(userIds1)}`);

      const isSenderInGroup = (userIds1 || []).some((u) => u.toString() === senderIdStr);
      if (!isSenderInGroup) {
        this.pendingGroupStart.delete(startStopKey);
        this.pendingGroupStop.delete(startStopKey);
        this.send27ToMe(msg);
        this.sendStartFailedToMe(msg);
        return;
      }

      // Overlap call filtering: check every member's call state in parallel.
      const checkPromises = (userIds1 || []).map((uid) => {
        if (uid.toString() === senderIdStr) { return Q.resolve(null); }
        const deferred = Q.defer();
        States.getCallDetailsForUser(uid, (detailsErr, details) => {
          // For a member shown as busy in a DIFFERENT group call, verify that group's floor
          // is still active. If the floor has no owner (call ended without cleanup — crash or
          // lost STOP), this member's active-call state is stale and they should be included.
          if (!detailsErr && details.inCall && details.channelType === 2 &&
              details.targetId !== msg.toId.toString()) {
            // Verify the member is still an active participant in the group they claim to be in.
            // getActiveParticipantsOfGroup has a Redis fallback — correct across all cluster workers.
            States.getActiveParticipantsOfGroup(details.targetId, (apErr, activeParticipants) => {
              const memberStillActive = !apErr && (activeParticipants || []).some(
                (p) => p.toString() === uid.toString()
              );
              if (!memberStillActive) {
                logger.info(`handleGroupStartMessage: member ${uid} has stale group-call state` +
                            ` for group ${details.targetId} (not in active participants) — clearing and including`);
                States.removeActiveParticipantFromAllGroups(uid);
                deferred.resolve({ uid, details: { ...details, inCall: false } });
              } else {
                deferred.resolve({ uid, details });
              }
            });
          } else {
            deferred.resolve({ uid, details });
          }
        });
        return deferred.promise;
      });

      Q.all(checkPromises).then((results) => {
        const availableRecipients: numberOrString[] = [];
        const overlapMissedRecipients: numberOrString[] = [];
        const offlineRecipients: numberOrString[] = [];
        const overrides: Array<(done: () => void) => void> = [];

        results.forEach((res: any) => {
          if (!res) { return; }
          const { uid, details } = res;
          // OFFLINE: member has no client record (app killed, Doze, lost socket).
          // Collected so the caller can wake them via push instead of silently dropping
          // them from the floor recipient list. Existing online members still get the call.
          if (!this.server.isUserConnected(uid)) {
            offlineRecipients.push(uid);
            return;
          }
          if (!details.inCall) {
            // Skip members who explicitly left this call session via DropCall,
            // UNLESS this is an SOS — emergencies reach everyone.
            // Dropped set is cleared by CallEndedForAll so they rejoin the next fresh call.
            if (States.isUserDroppedFromGroup(uid, msg.toId) && !isSos) {
              logger.info(`handleGroupStartMessage: skipping dropped member ${uid} for group ${msg.toId}`);
            } else {
              availableRecipients.push(uid);
            }
          } else if (details.channelType === 2 && details.targetId === msg.toId.toString()) {
            // Already in this same group call — include them.
            availableRecipients.push(uid);
            // Override only if the existing call is genuinely non-SOS. details.isSos can be
            // stale (e.g. reset to false on reconnect) so also check groupSosState directly.
            const memberGroupInSos = details.isSos || States.isGroupSos(details.targetId);
            if (isSos && !memberGroupInSos) {
              // SOS overrides a normal call even in the same group to ensure UI visibility.
              overrides.push((done) => this.executeCallOverrideForUser(uid, details, done));
            }
          } else if (isSos && !details.isSos && !States.isGroupSos(details.targetId)) {
            // SOS overrides a non-SOS call in a different channel.
            // Guard: also check groupSosState in case details.isSos is stale — don't eject
            // a member from an SOS group call just because their per-user flag is stale.
            availableRecipients.push(uid);
            overrides.push((done) => this.executeCallOverrideForUser(uid, details, done));
          } else {
            // If the member's "busy" state is against the SDK heartbeat target (00000 or all
            // zeros), that is stale state from a connection heartbeat — clear it and include them.
            const targetIdStr = (details.targetId || "").toString().replace(/^0+$/, "0");
            if (targetIdStr === "0" || targetIdStr === "00000") {
              logger.info(`handleGroupStartMessage: member ${uid} has stale 00000 private call` +
                          ` state — clearing and including in group call`);
              States.clearUserPrivateCall(uid);
              States.clearUserPrivateCall(details.targetId);
              availableRecipients.push(uid);
            } else {
              logger.info(`handleGroupStartMessage: skipping busy member ${uid}` +
                          ` (sos=${details.isSos} newSos=${isSos}` +
                          ` theirChannel=${details.channelType} theirTarget=${details.targetId})`);
              overlapMissedRecipients.push(uid);
            }
          }
        });

        overlapMissedRecipients.forEach((uid) => this.sendOverlapMissedCallText(msg, uid));

        // ── Step 2: If ALL members are busy, reject immediately — no floor acquired yet.
        if (availableRecipients.length === 0 && overrides.length === 0) {
          this.pendingGroupStart.delete(startStopKey);
          this.pendingGroupStop.delete(startStopKey);
          logger.info(`handleGroupStartMessage: all members busy for group ${msg.toId}` +
                      ` — rejecting ${msg.fromId} before floor acquisition`);
          this.sendStartFailedToMe(msg);
          this.sendBusyEventText(msg, "Busy");
          return;
        }

        const acquireFloorAndProceed = () => {
          // ── Step 3: At least one member is available — acquire the floor and proceed.
          this.acknowledgeGroupStartMessage(msg, (err, acknowledged) => {
            this.pendingGroupStart.delete(startStopKey);

            if (err) { debug(`id: ${this.id} acknowledgeGroupMessage: groupId: ${msg.toId}` +
                             ` id: ${msg.fromId} err: ${err}`); }
            if (!acknowledged) {
              // Floor was busy (race with another caller) — discard any buffered STOP.
              this.pendingGroupStop.delete(startStopKey);
              return;
            }

            // Fix #3: Only set the group SOS flag to true; never clear it while the
            // session is active. Receivers PTT with isSos=false (the Android client strips
            // the flag for non-callers so the router doesn't re-trigger SOS preemption on
            // every receiver press). Without this guard a receiver's first PTT would reset
            // setGroupSos(groupId, false), breaking SOS-priority for late-joining members
            // and future overlap detection for that group for the rest of the session.
            if (isSos) { States.setGroupSos(msg.toId, true); }
            // If the sender was in a different group call before starting this one,
            // clean up their old group membership so remaining members aren't left
            // with stale state (stale floor owner, wrong participant count).
            States.getActiveCallGroupsOfUser(msg.fromId, (gsErr, prevGroups) => {
              (prevGroups || []).forEach((prevGroupId) => {
                if (prevGroupId.toString() !== msg.toId.toString()) {
                  States.releaseFloorOfGroup(prevGroupId, msg.fromId + "");
                  States.removeUserFromActiveCallGroup(msg.fromId, prevGroupId);
                }
              });
            });
            States.setUserGroupCallState(msg.fromId, msg.toId, isSos);
            States.addUserToActiveCallGroup(msg.fromId, msg.toId, isSos);

            Recorder.start(msg);

            // Block AUDIO until recipient list is committed (avoids early audio leaks).
            const floorRecipientEpoch = States.setGroupFloorRecipients(msg.toId, msg.fromId, []);

            const proceed = () => {
              // Commit the recipients list and broadcast START to available members.
              States.setGroupFloorRecipients(msg.toId, msg.fromId, availableRecipients.map((r) => r + ""));
              this.server.sendMessageToGroupSubset(msg, availableRecipients);

              // Notify caller about offline members so they can send wake-up pushes.
              // Online members already hear the call; offline members will be reachable
              // for the caller's next PTT once they wake up and re-register.
              if (offlineRecipients.length > 0) {
                this.sendOfflineMembersText(msg, offlineRecipients);
              }

              // Process a buffered STOP so quick tap-and-release always sends START then STOP.
              // Delay by 400ms so the receiver has time to process the START before STOP arrives —
              // without this, both messages land near-simultaneously and the receiver's async
              // handleIncomingCall() coroutine is aborted by the guard before it can broadcast
              // the call UI, leaving the receiver never seeing the call at all.
              const bufferedStop = this.pendingGroupStop.get(startStopKey);
              if (bufferedStop) {
                this.pendingGroupStop.delete(startStopKey);
                logger.info(`handleGroupStartMessage: processing buffered STOP for` +
                            ` user ${msg.fromId} group ${msg.toId} (quick tap-and-release, 400ms delay)`);
                setTimeout(() => this.finishStopMessage(bufferedStop), 400);
              }
            };

            if (overrides.length > 0) {
              Q.all(overrides.map((o) => {
                const d = Q.defer();
                o(() => d.resolve(null));
                return d.promise;
              })).then(() => proceed());
            } else {
              proceed();
            }

            // Safety: clear floor recipients if proceed never runs (defensive).
            // void floorRecipientEpoch; // Removed to fix lint error
          });
        };

        // Both SOS and interrupt must preempt the current floor owner; otherwise
        // acknowledgeGroupStartMessage will fail to acquire the floor and the call
        // is silently dropped while someone is speaking.
        if (!isInterrupt && !isSos) {
          acquireFloorAndProceed();
          return;
        }

        States.getBusyStateOfGroup(msg.toId, (ownerErr, floorOwnerId) => {
          if (ownerErr) {
            logger.info(`handleGroupStartMessage: failed to inspect floor owner for SOS/interrupt` +
                        ` group ${msg.toId} err ${ownerErr}`);
            acquireFloorAndProceed();
            return;
          }

          const floorOwnerIdStr = floorOwnerId ? floorOwnerId.toString() : "0";
          if (floorOwnerIdStr === "0" || floorOwnerIdStr === senderIdStr) {
            acquireFloorAndProceed();
            return;
          }

          logger.info(`handleGroupStartMessage: ${isSos ? "SOS" : "interrupt"} START from ${msg.fromId}` +
                      ` preempting floor owner ${floorOwnerIdStr} in group ${msg.toId}`);
          this.forceStopCurrentGroupFloorOwner(msg.toId, floorOwnerIdStr, acquireFloorAndProceed);
        });
      });
    });
  }

  private withOperationDedupe(
    this: Client,
    operationId: string | null,
    msg: IMessage,
    actionType: "START" | "STOP",
    action: () => void
  ): void {
    if (!operationId) {
      action();
      return;
    }
    const scopedOperationId = `${this.id}:${actionType}:${msg.channelType}:${msg.toId}:${operationId}`;
    Redis.reserveOperation(scopedOperationId, undefined, (err, reserved) => {
      if (err) {
        logger.error(`withOperationDedupe reserveOperation error opId ${scopedOperationId} err ${err}`);
        action();
        return;
      }
      if (!reserved) {
        logger.info(`withOperationDedupe duplicate operation opId ${scopedOperationId} — proceeding to ensure ACK`);
      }
      action();
    });
  }

  private extractOperationId(this: Client, msg: IMessage): string | null {
    if (msg.messageId && typeof msg.messageId === "string") {
      try {
        const meta = JSON.parse(msg.messageId);
        if (meta && typeof meta.operationId === "string" && meta.operationId.length > 0) {
          return meta.operationId;
        }
      } catch (e) { /* no-op */ }
    }
    if (msg.payload && typeof msg.payload === "string") {
      try {
        const payload = JSON.parse(msg.payload);
        if (payload && typeof payload.operationId === "string" && payload.operationId.length > 0) {
          return payload.operationId;
        }
      } catch (e) { /* no-op */ }
    }
    return null;
  }

  private acknowledgeGroupStartMessage(this: Client, msg: IMessage, callback:
    (error: Error, acknowledged: boolean) => void): void {
    debug(`id ${this.id} acknowledgeGroupStartMessage => ${JSON.stringify(msg)}`);
    if (msg.messageType !== MessageType.START) {
      // don't send ACK, callback pass through.
      if (callback) { return callback(null, true); }
    }

    debug(`id ${this.id} Starting to acquire floor`);
    Q.Promise((resolve, reject) => {
      States.acquireFloorOfGroup(msg.toId, msg.fromId, (err, acquired, floorOwner) => {
        if (err) { return reject(err); }
        if (acquired) {
          States.setCurrentMessageOfGroup(msg.toId, msg, function(setErr) {
            if (setErr) { return reject(setErr); }
            return resolve(false);
          });
        } else {
          const ownerStr = (floorOwner || "").toString();
          if (ownerStr && ownerStr !== "0") {
            // Cluster-safe orphan check: verify the floor owner is still an active participant
            // in this group. getActiveParticipantsOfGroup has a Redis fallback so it works
            // correctly across all worker processes.
            return States.getActiveParticipantsOfGroup(msg.toId, (apErr, activeParticipants) => {
              const ownerStillActive = !apErr && (activeParticipants || []).some(
                (p) => p.toString() === ownerStr
              );
              if (!ownerStillActive) {
                logger.info(`acknowledgeGroupStartMessage: group ${msg.toId} floor owner ${ownerStr}` +
                            ` is not an active participant — force-releasing orphaned floor and retrying`);
                return States.releaseFloorOfGroup(msg.toId, ownerStr, () => {
                  States.acquireFloorOfGroup(msg.toId, msg.fromId, (retryErr, retryAcquired) => {
                    if (retryErr) { return reject(retryErr); }
                    if (!retryAcquired) { return resolve(true); }
                    States.setCurrentMessageOfGroup(msg.toId, msg, (setErr) => {
                      if (setErr) { return reject(setErr); }
                      return resolve(false);
                    });
                  });
                });
              }
              debug(`id ${this.id} floor busy for group ${msg.toId} owner ${floorOwner}`);
              return resolve(true);
            });
          }
          debug(`id ${this.id} floor busy for group ${msg.toId} owner ${floorOwner}`);
          return resolve(true);
        }
      });
    }).then((busy: boolean) => {

      let payload: string;
      let messageType: number;
      if (busy) {
        debug(`id ${this.id} Response START_FAILED`);
        payload = "Busy";
        messageType = MessageType.START_FAILED;
      } else {
        debug(`id ${this.id} Response START_ACK`);
        payload = msg.payload ? msg.payload.toString() : "Acknowledged";
        messageType = MessageType.START_ACK;
      }

      this.server.sendMessageToUser({
        channelType: msg.channelType,
        fromId: msg.fromId,
        messageType,
        payload,
        toId: msg.toId
      }, msg.fromId);

      callback(null, !busy);
    }, (err) => {
      // Redis error during floor acquisition — send an explicit START_FAILED so the client
      // gets immediate feedback instead of hanging until ACK_START Timeout (code=4).
      logger.error(`acknowledgeGroupStartMessage: groupId ${msg.toId} from ${msg.fromId} err ${err}`);
      this.server.sendMessageToUser({
        channelType: msg.channelType,
        fromId: msg.fromId,
        messageType: MessageType.START_FAILED,
        payload: "Busy",
        toId: msg.toId
      }, msg.fromId);
      return callback(null, false);
    });
  }

  private handleConnectionTest(this: Client, msg: IMessage) {
    const connectionTestAckMsg = {
      channelType: ChannelType.PRIVATE,
      fromId: 0,
      messageType: MessageType.CONNECTION_ACK,
      payload: msg.payload,
      toId: msg.fromId
    };
    this.emit("message", connectionTestAckMsg, this);
  }

  // CONNECTION EVENT HANDLERS

  private handleConnectionClose = (connection: Connection) => {
    delete this.connections[connection.key];
    if (Object.keys(this.connections).length <= 0) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
      States.getGroupsWithActiveParticipant(this.id, (err, activeGroups) => {
        const continueWithCloseHandling = () => {
          States.releaseFloorOwnershipForUser(this.id);
          States.releasePrivateFloorOwnershipForUser(this.id);
          // Check private-call state first.  "unregister" MUST be emitted INSIDE this
          // callback so the server's "message" event listener is still attached when
          // sendEndCallToUser fires — moving emit("unregister") outside would cause the
          // listener to be removed before the EndCall message is dispatched.
          States.getCallDetailsForUser(this.id, (err2, details) => {
            if (!err2 && details.inCall && details.channelType === 1) {
              // Guard against stale "00000" peer — sending DROP to an invalid target causes
              // unnecessary lookup noise and signals a ghost user that never existed.
              const targetIdStr = (details.targetId || "").toString().replace(/^0+$/, "0");
              if (details.targetId && targetIdStr !== "0" && targetIdStr !== "00000") {
                // releasePrivateFloorOwnershipForUser(this.id) already ran above, so if the
                // peer still holds the floor they are the active talker. Skip DropCall to avoid
                // cutting off a transmission in progress — let them finish and release PTT naturally.
                States.isPrivateFloorOwner(this.id, details.targetId, details.targetId, (floorErr, peerIsOwner) => {
                  if (!peerIsOwner) {
                    // Disconnecting user was the talker (or floor was idle). Notify peer so their UI resets.
                    this.sendDropCallToUser(this.id, details.targetId, 1, 0);
                    States.clearUserPrivateCall(details.targetId);
                  } else {
                    logger.info(`handleConnectionClose: peer ${details.targetId} holds private floor —` +
                                ` skipping DropCall, clearing only self ${this.id}`);
                  }
                  States.clearUserActiveCall(this.id);
                  // Emit unregister AFTER sendEndCallToUser so the server's "message"
                  // listener is still active when the EndCall event is dispatched.
                  this.emit("unregister", this, activeGroups || []);
                });
                return;
              }
            }
            States.clearUserActiveCall(this.id);
            // Emit unregister AFTER sendEndCallToUser so the server's "message"
            // listener is still active when the EndCall event is dispatched.
            this.emit("unregister", this, activeGroups || []);
          });
        };

        // Only remove from groups where the user does NOT hold the floor.
        // If they hold the floor they are currently talking — removing them would end
        // their transmission when a listener reconnects.
        if (activeGroups && activeGroups.length > 0) {
          const groupsToKeep: Array<number|string> = [];
          let groupsChecked = 0;
          activeGroups.forEach((groupId) => {
            States.isFloorOwnerOfGroup(groupId, this.id, (floorErr, isOwner) => {
              if (isOwner) {
                logger.info(`handleConnectionClose: user ${this.id} holds group floor in ${groupId}` +
                            ` — keeping as active participant, will clear on STOP`);
                groupsToKeep.push(groupId);
              }
              groupsChecked++;
              if (groupsChecked === activeGroups.length) {
                // Remove from all groups, then re-add groups where user holds floor.
                // Preserve the SOS flag from groupSosState so a disconnecting SOS caller
                // doesn't appear as isSos=false and get overridden by a second SOS.
                States.removeActiveParticipantFromAllGroups(this.id, (removeErr) => {
                  groupsToKeep.forEach((groupToKeep) => {
                    States.addUserToActiveCallGroup(this.id, groupToKeep, States.isGroupSos(groupToKeep));
                  });
                  continueWithCloseHandling();
                });
              }
            });
          });
        } else {
          States.removeActiveParticipantFromAllGroups(this.id);
          continueWithCloseHandling();
        }
      });
    }
  }

  private handleConnectionMessage = (msg: IMessage) => {
    try {
      if (msg.channelType === ChannelType.PRIVATE) {
        this.handlePrivateMessage(msg);
      } else if (msg.channelType === ChannelType.GROUP) {
        this.handleGroupMessage(msg);
      } else {
        debug(`id ${this.id} handleConnectionMessage UNKNOWN ${JSON.stringify(msg)}`);
      }
    } catch (error) {
      console.error(error);
    }
  }

  private handleConnectionPong = (payload: string) => {
    debug(`id ${this.id} handleConnectionPong ${payload}`);
    this.emit("pong");
  }

  private send27ToMe = (msg: IMessage) => {
    debug(`id ${this.id} Sending UNAUTHORIZED_GROUP to user: ${msg.fromId} group: ${msg.toId}`);
    this.message({
      channelType: ChannelType.GROUP,
      fromId: msg.fromId,
      messageType: MessageType.UNAUTHORIZED_GROUP,
      payload: "Unauthorized Group",
      toId: msg.toId
    });
  }

  private sendStartFailedToMe = (msg: IMessage) => {
    debug(`id ${this.id} Sending START_FAILED to user: ${msg.fromId} group: ${msg.toId}`);
    this.message({
      channelType: ChannelType.GROUP,
      fromId: msg.fromId,
      messageType: MessageType.START_FAILED,
      payload: "Ack Failed",
      toId: msg.toId
    });
  }
}

export interface IClients {
  [index: string]: Client;
  [index: number]: Client;
}
