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
    debug(`id ${this.id} handlePrivateStartMessage ${JSON.stringify(msg)}`);

    const newCallIsSos = this.parseIsSosCall(msg);

    // 1. Check if the SENDER is already in a different active call.
    States.getCallDetailsForUser(msg.fromId, (err1, senderDetails) => {
      if (err1) {
        logger.info(`handlePrivateStartMessage: sender ${msg.fromId} lookup error: ${err1}`);
      }

      if (!err1 && senderDetails.inCall) {
        // If the sender is busy with someone else, they can't start a new call.
        if (senderDetails.targetId.toString() !== msg.toId.toString()) {
          // Before rejecting, check whether the peer the sender is "in a call with"
          // is actually still connected.  If not, the state is stale (previous call
          // ended without a clean EndCall) — clear it and let this call through.
          if (!this.server.isUserConnected(senderDetails.targetId)) {
            logger.info(`handlePrivateStartMessage: clearing stale private call state for` +
                        ` ${msg.fromId} (was linked to disconnected peer ${senderDetails.targetId})`);
            States.clearUserPrivateCall(msg.fromId);
          } else {
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
      }

      // 2. Check if the TARGET user is already in an active call.
      States.getCallDetailsForUser(msg.toId, (err2, targetDetails) => {
        if (err2) {
          logger.info(`handlePrivateStartMessage: target ${msg.toId} lookup error: ${err2}`);
        }

        logger.info(`handlePrivateStartMessage check: target=${msg.toId} inCall=${targetDetails.inCall}` +
                    ` targetBusyWith=${targetDetails.targetId} sender=${msg.fromId}`);

        if (targetDetails.inCall) {
          // If the target is busy WITH THE SENDER, allow the call (same session heartbeat).
          if (targetDetails.targetId.toString() === msg.fromId.toString()) {
            logger.info(`handlePrivateStartMessage: continuing existing session between ${msg.fromId} and ${msg.toId}`);
            this.proceedWithPrivateStart(msg, newCallIsSos);
            return;
          }

          if (!newCallIsSos || targetDetails.isSos) {
            // Reject: normal→any or sos→sos
            logger.info(`handlePrivateStartMessage: target ${msg.toId} busy (existingSos=${targetDetails.isSos}` +
                        ` newSos=${newCallIsSos}) — rejecting ${msg.fromId}`);
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

          // SOS overrides a normal call: end the target's existing call first.
          logger.info(`handlePrivateStartMessage: SOS override — ending existing call for ${msg.toId}`);
          this.executeCallOverrideForUser(msg.toId, targetDetails, () => {
            this.proceedWithPrivateStart(msg, newCallIsSos);
          });
          return;
        }

        this.proceedWithPrivateStart(msg, newCallIsSos);
      });
    });
  }

  private proceedWithPrivateStart(this: Client, msg: IMessage, isSos: boolean) {
    // Acquire the private-channel floor before allowing the call to proceed.
    // This is synchronous so it is atomic within a single server process:
    // if both users press simultaneously, only the first START wins.
    States.acquirePrivateFloor(msg.fromId, msg.toId, msg.fromId, (err, acquired, currentOwner) => {
      if (!acquired) {
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
      // Notify BOTH sides of the private call so their UIs reset.
      this.sendDropCallToUser(userId, currentCall.targetId, 1, 0);
      this.sendDropCallToUser(currentCall.targetId, userId, 1, 0);
      // IMPORTANT: must call callback() here so that Q.all(overrides) resolves
      // and proceed() forwards the SOS START to the group.  Without this the
      // SOS call is silently swallowed and the group floor leaks until TTL.
      callback();
    } else { // GROUP
      const groupId = currentCall.targetId;
      // Clear the user's Redis active-call key immediately so subsequent overlap
      // checks don't see them as still busy in the group they are being ejected from.
      States.clearUserActiveCall(userId);
      States.removeUserFromActiveCallGroup(userId, groupId, (err, count) => {
        // 1. Notify the specific user to reset their UI
        this.sendDropCallToUser("System", groupId, 2, count, userId);

        // 2. Notify the rest of the group about the participant drop
        this.sendDropCallToUser(userId, groupId, 2, count);

        // Invoke callback only after async cleanup is done so the new SOS call
        // doesn't start connecting before this user has been fully ejected.
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
    deliveryId?: numberOrString
  ) {
    const dropMsg = {
      channelType,
      fromId,
      messageId: JSON.stringify({
        callId: toId.toString(),
        errorType: "",
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

  /** Parses the isSosCall flag from the START message payload. */
  private parseIsSosCall(this: Client, msg: IMessage): boolean {
    try {
      const data = JSON.parse(msg.payload.toString());
      return data.isSosCall === true || data.onlyConnectCallOnLongPress === true;
    } catch {
      return false;
    }
  }

  private acknowledgePrivateStartMessage(this: Client, msg: IMessage) {
    const payload = msg.payload || "Acknowledged";
    this.message({
      ...msg,
      channelType: msg.channelType,
      messageType: MessageType.START_ACK,
      payload
    });
  }

  // PRIVATE & GROUP (USED BY BOTH) MESSAGE HANDLERS

  private handleStopMessage(this: Client, msg: IMessage) {
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
        debug(`id ${this.id} ignoring STOP from non-owner ${msg.fromId} for private ${msg.toId}`);
        return;
      }
      // Note: We don't clearUserPrivateCall here anymore. Session persistency relies on Redis TTL
      // to bridge the "hang time" gap between PTT turns.
      return this.finishStopMessage(msg);
    });
  }

  private finishStopMessage(this: Client, msg: IMessage) {
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
            States.clearGroupFloorRecipients(msg.toId);
            States.removeCurrentMessageOfGroup(msg.toId);
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
    this.message({
      ...msg,
      messageId,
      messageType: MessageType.STOP_ACK,
      payload: "Acknowledged"
    });
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
          meta.membersInCall = count;
          msg.messageId = JSON.stringify(meta);
          callback();
        });
        return;
      case "CallEndedForAll":
        States.clearActiveCallGroup(groupId, () => {
          meta.membersInCall = 0;
          msg.messageId = JSON.stringify(meta);
          callback();
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
    States.isFloorOwnerOfGroup(msg.toId, msg.fromId, (ownerErr, isOwner) => {
      if (ownerErr) {
        debug(`id: ${this.id} handleGroupAudioMessage owner check err: ${ownerErr}`);
        return;
      }
      if (!isOwner) {
        debug(`id: ${this.id} dropping group AUDIO from non-owner ${msg.fromId} for group ${msg.toId}`);
        return;
      }
      States.refreshFloorOfGroup(msg.toId, msg.fromId);
      Recorder.resume(msg, (err, messageId, duration) => {
        if (err) { debug(`id: ${this.id} recorder.resume: err: ${err} messageId: ${messageId}` +
                         ` duration: ${duration}`); }

        // Use the floor-session recipient list when available (overlap-aware subset delivery).
        // undefined → normal call, broadcast to everyone in the group.
        // [] (empty) → all members were busy; drop this audio packet.
        // [...ids]  → partial availability; deliver only to the members who received START.
        const recipients = States.getGroupFloorRecipients(msg.toId);
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
    // Register the in-flight START before any async work so that a STOP arriving
    // during floor acquisition (quick tap-and-release) can be buffered instead of
    // silently dropped.
    const startStopKey = `${msg.fromId}_${msg.toId}`;
    this.pendingGroupStart.set(startStopKey, msg);

    this.acknowledgeGroupStartMessage(msg, (err, acknowledged) => {
      this.pendingGroupStart.delete(startStopKey);

      if (err) { debug(`id: ${this.id} acknowledgeGroupMessage: groupId: ${msg.toId}` +
                       ` id: ${msg.fromId} err: ${err}`); }
      if (!acknowledged) {
        // Floor was busy or START failed — discard any buffered STOP.
        this.pendingGroupStop.delete(startStopKey);
        return;
      }

      const isSos = this.parseIsSosCall(msg);
      States.setGroupSos(msg.toId, isSos);
      States.setUserGroupCallState(msg.fromId, msg.toId, isSos);
      States.addUserToActiveCallGroup(msg.fromId, msg.toId, isSos);

      Recorder.start(msg);

      // Block all AUDIO immediately so no packets leak while the async member check runs.
      // Will be updated to the actual subset (or cleared) once availability is known.
      States.setGroupFloorRecipients(msg.toId, []);

      States.getUsersInsideGroup(msg.toId, (err1, userIds1) => {
        debug(`handleGroupStartMessage - getUsersInsideGroup groupId: ${msg.toId} users: ${JSON.stringify(userIds1)}`);
        const senderIdStr = msg.fromId.toString();
        const isSenderInGroup = (userIds1 || []).some((u) => u.toString() === senderIdStr);

        if (!isSenderInGroup) {
          this.send27ToMe(msg);
          this.sendStartFailedToMe(msg);
        }

        // Overlap call filtering: Check every member's global state in Redis (asynchronously)
        const checkPromises = (userIds1 || []).map((uid) => {
          if (uid.toString() === senderIdStr) { return Q.resolve(null); }
          const deferred = Q.defer();
          States.getCallDetailsForUser(uid, (detailsErr, details) => {
            deferred.resolve({ uid, details });
          });
          return deferred.promise;
        });

        Q.all(checkPromises).then((results) => {
          const availableRecipients: numberOrString[] = [];
          const overrides: Array<(done: () => void) => void> = [];

          results.forEach((res: any) => {
            if (!res) { return; }
            const { uid, details } = res;
            if (!details.inCall) {
              // Member is free — include them.
              availableRecipients.push(uid);
            } else if (details.channelType === 2 && details.targetId === msg.toId.toString()) {
              // Member is already in THIS same group call — they should receive the
              // new floor owner's audio.  Do NOT treat this as "busy" — that would
              // lock out everyone already in the call from hearing new PTT presses.
              availableRecipients.push(uid);
            } else if (isSos && !details.isSos) {
              // SOS overrides a normal call in a different channel.
              availableRecipients.push(uid);
              overrides.push((done) => this.executeCallOverrideForUser(uid, details, done));
            } else {
              logger.info(`handleGroupStartMessage: skipping busy member ${uid}` +
                          ` (sos=${details.isSos} newSos=${isSos}` +
                          ` theirChannel=${details.channelType} theirTarget=${details.targetId})`);
            }
          });

          const proceed = () => {
            if (availableRecipients.length > 0) {
              // Update recipients to the actual subset so AUDIO is delivered only to them.
              States.setGroupFloorRecipients(msg.toId, availableRecipients.map((r) => r + ""));
              this.server.sendMessageToGroupSubset(msg, availableRecipients);
            } else {
              // All members are busy — notify the caller, release the floor, and undo
              // the group-state entries that were created at the top of this handler
              // (addUserToActiveCallGroup / setUserGroupCallState) so the caller does
              // not appear as "in call" after a rejected attempt.
              logger.info(`handleGroupStartMessage: all members busy for group ${msg.toId}` +
                          ` — sending BusyEvent to ${msg.fromId}`);
              this.sendBusyEventText(msg, "Busy");
              States.cancelGroupStart(msg.fromId, msg.toId);
              States.releaseFloorOfGroup(msg.toId, msg.fromId, () => {
                States.clearGroupFloorRecipients(msg.toId);
                States.removeCurrentMessageOfGroup(msg.toId);
              });
            }

            // Process a buffered STOP only AFTER the START has been dispatched so that
            // clients always receive START before STOP (preserves quick tap-and-release).
            const bufferedStop = this.pendingGroupStop.get(startStopKey);
            if (bufferedStop) {
              this.pendingGroupStop.delete(startStopKey);
              logger.info(`handleGroupStartMessage: processing buffered STOP for` +
                          ` user ${msg.fromId} group ${msg.toId} (quick tap-and-release)`);
              this.finishStopMessage(bufferedStop);
            }
          };

          if (overrides.length > 0) {
            Q.all(overrides.map((o) => {
              const d = Q.defer();
              o(() => d.resolve(null));
              return d.promise;
            })).then(proceed);
          } else {
            proceed();
          }
        });
      });
    });
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

      this.message({
        channelType: msg.channelType,
        fromId: msg.fromId,
        messageType,
        payload,
        toId: msg.toId
      });

      callback(null, !busy);
    }, (err) => {
      debug(`id ${this.id} acknowledgeGroupStartMessage groupId ${msg.toId} ERR ${err}`);
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
        States.removeActiveParticipantFromAllGroups(this.id);
        States.releaseFloorOwnershipForUser(this.id);
        States.releasePrivateFloorOwnershipForUser(this.id);
        // Check private-call state first.  "unregister" MUST be emitted INSIDE this
        // callback so the server's "message" event listener is still attached when
        // sendEndCallToUser fires — moving emit("unregister") outside would cause the
        // listener to be removed before the EndCall message is dispatched.
        States.getCallDetailsForUser(this.id, (err2, details) => {
          if (!err2 && details.inCall && details.channelType === 1) {
            // Notify the peer that this user disconnected so their UI resets and
            // they stop transmitting audio to a now-dead socket.
            this.sendDropCallToUser(this.id, details.targetId, 1, 0);
            States.clearUserPrivateCall(details.targetId);
          }
          States.clearUserActiveCall(this.id);
          // Emit unregister AFTER sendEndCallToUser so the server's "message"
          // listener is still active when the EndCall event is dispatched.
          this.emit("unregister", this, activeGroups || []);
        });
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
