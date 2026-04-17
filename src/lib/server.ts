import * as cluster from "cluster";

import * as dbug from "debug";
import * as _ from "lodash";
import * as Q from "q";
import * as WebSocket from "ws";

import ChannelType = require("./channeltype");
import Client, { IClients } from "./client";
import config = require("./config");

import logger = require("./logger");
import MessageType = require("./messagetype");
import { packer } from "./packer";
import Redis = require("./redis");
import States from "./states";
import { IMessage, numberOrString } from "./types";

const WORKER_NUMBER = cluster.worker ? cluster.worker.id : "-";
const REPLAY_MAX_AGE_MS = 10_000; // Reduced from 20_000 for faster cleanup
const REPLAY_MAX_EVENTS = 20; // Reduced from 40 for lower memory usage
const MESSAGE_BATCH_SIZE = 50; // Batch processing for better throughput
const dbug1 = dbug("vp:router");
function debug(msg: string) {
  dbug1((cluster.worker ? `worker ${cluster.worker.id} ` : "") + msg);
}

export interface IServer {
  sendMessageToUser: (message: IMessage, deliveryId?: numberOrString) => void;
  sendMessageToGroup: (message: IMessage) => void;
  sendMessageToGroupSubset: (message: IMessage, recipientIds: numberOrString[]) => void;
  isUserConnected: (userId: numberOrString) => boolean;
}
interface IConnection {
  token: string;
  deviceId: string;
  key: string;
  lastAckedSeq?: number;
}

// class Server implements IServer {
class Server implements IServer {

  private clients: IClients = {};
  private sockets = {};
  private deviceTokens = {};
  private wss = null;
  private verify = null;

  constructor(options) {
    const opts = {
      memo: null,
      port: 9000,
      server: null,
      verify: this.verifyClient,
      ...options
    };

    if (opts.verify) { this.verify = opts.verify; }

    States.setMemored(opts.memo);
    States.periodicInspect();
    if (WORKER_NUMBER.toString() === "1") {
        Redis.periodicClean();
    }

    // WSS & WS SETUP
    if (opts.server) {
      this.wss = new WebSocket.Server({ server: opts.server, verifyClient: this.verify.bind(this) });
      logger.info("WebSocket.Server is created");
    } else {
      this.wss = new WebSocket.Server({ port: opts.port, verifyClient: this.verify.bind(this) });
      logger.info(`WebSocket.Server is created at port ${opts.port}`);
    }

    this.wss.on("connection", this.handleWssConnection.bind(this));
  }

  // IServer Implementation

  public isUserConnected(this: Server, userId: numberOrString): boolean {
    return this.clients.hasOwnProperty(userId + "");
  }

  public sendMessageToUser(this: Server, msg: IMessage, deliveryId?: numberOrString) {
    const dest = deliveryId || msg.toId;
    const persistAndSend = (enrichedMsg: IMessage) => {
      packer.pack(enrichedMsg, (err, packed) => {
        const client = this.clients[dest];
        if (!client) {
          if (msg.messageType === MessageType.AUDIO) {
            debug(`sendMessageToUser type AUDIO NOT-FOUND id ${dest} ${JSON.stringify(msg)}`);
          } else {
            debug(`sendMessageToUser type NON-AUDIO NOT-FOUND id ${dest} ${JSON.stringify(msg)}`);
          }
          return;
        }
        client.send(packed);
      });
    };

    if (!this.shouldPersistForReplay(msg)) {
      persistAndSend(msg);
      return;
    }

    Redis.nextSignalingSeq(dest, (seqErr, seq) => {
      if (seqErr) {
        logger.error(`sendMessageToUser nextSignalingSeq error dest ${dest} err ${seqErr}`);
        persistAndSend(msg);
        return;
      }
      const enriched = this.attachServerSequence(msg, seq);
      Redis.pushSignalingOutboxEvent(dest, JSON.stringify(enriched), (outboxErr) => {
        if (outboxErr) {
          logger.error(`sendMessageToUser pushSignalingOutboxEvent error dest ${dest} err ${outboxErr}`);
        }
        persistAndSend(enriched);
      });
    });
  }

  public sendMessageToGroup(this: Server, msg: IMessage) {
    this.applyAuthoritativeGroupMeta(msg, () => {
      this.prepareGroupMessage(msg, (packed) => {
        this.sendDataFromUserToGroup(packed, msg.fromId, msg.toId, this.shouldEchoToSender(msg));
      });
    });
  }

  public sendMessageToGroupSubset(this: Server, msg: IMessage, recipientIds: numberOrString[]) {
    this.applyAuthoritativeGroupMeta(msg, () => {
      this.prepareGroupMessage(msg, (packed) => {
        this.sendDataFromUserToSubset(packed, msg.fromId, recipientIds, this.shouldEchoToSender(msg));
      });
    });
  }

  private prepareGroupMessage(this: Server, msg: IMessage, callback: (packed: Buffer) => void) {
    const messageIdForLog = msg.messageType === MessageType.TEXT ||
      msg.messageType === MessageType.INTERACTIVE ||
      msg.messageType === MessageType.START ||
      msg.messageType === MessageType.STOP
      ? msg.messageId
      : "[omitted]";
    logger.info(
      `prepareGroupMessage from: ${msg.fromId} to: ${msg.toId}` +
      ` messageType: ${msg.messageType} messageId: ${messageIdForLog}`
    );
    packer.pack(msg, (err, packed) => {
      callback(packed);
    });
  }

  private shouldEchoToSender(msg: IMessage): boolean {
    try {
      const meta = JSON.parse(msg.messageId as string);
      return !!(meta && meta.textMessageType === "JoinAcknowledgement");
    } catch (e) {
      return false;
    }
  }

  private applyAuthoritativeGroupMeta(this: Server, msg: IMessage, callback: () => void) {
    if (
      msg.channelType !== ChannelType.GROUP ||
      (msg.messageType !== MessageType.TEXT && msg.messageType !== MessageType.INTERACTIVE) ||
      !msg.messageId ||
      typeof msg.messageId !== "string"
    ) {
      callback();
      return;
    }

    let meta;
    try {
      meta = JSON.parse(msg.messageId);
    } catch (error) {
      callback();
      return;
    }

    if (!meta || !meta.textMessageType) {
      callback();
      return;
    }

    if (meta.textMessageType === "CallEndedForAll") {
      meta.membersInCall = 0;
      msg.messageId = JSON.stringify(meta);
      callback();
      return;
    }

    if (meta.textMessageType !== "JoinAcknowledgement" && meta.textMessageType !== "DropCall") {
      callback();
      return;
    }

    States.getActiveParticipantCountOfGroup(msg.toId, (err, count) => {
      meta.membersInCall = count;
      msg.messageId = JSON.stringify(meta);
      callback();
    });
  }

  private handleClientUnregister = (client: Client, activeGroups?: Array<number|string>) => {
    const clientId = client.id;
    if (!this.clients[clientId]) { return; }

    (activeGroups || []).forEach((groupId) => {
      States.getActiveParticipantCountOfGroup(groupId, (err, count) => {
        logger.info(`PARTICIPANT_COUNT disconnect groupId ${groupId} user ${clientId} count ${count}`);

        let safeCallId = groupId + "";
        if (safeCallId.startsWith("TELENET_")) {
          safeCallId = safeCallId.substring("TELENET_".length);
        }

        const message = {
          channelType: ChannelType.GROUP,
          fromId: clientId,
          messageId: JSON.stringify({
            callId: safeCallId,
            errorType: "",
            lang: "ja-JP",
            membersInCall: count,
            textMessageType: "DropCall",
            translate: false
          }),
          messageType: MessageType.TEXT,
          payload: JSON.stringify({
            message_id: Date.now().toString(),
            text: ""
          }),
          toId: groupId
        };
        this.sendMessageToGroup(message);
      });
    });

    client.removeListener("message", this.handleClientMessage);
    client.removeListener("unregister", this.handleClientUnregister);
    delete this.clients[clientId];
    delete this.sockets[clientId];
    logger.info(`UNREGISTERED id ${clientId} clients ${Object.keys(this.clients).length}` +
                ` sockets ${Object.keys(this.sockets).length} wss ${this.wss.clients.size}`);
  }

  private handleClientMessage = (msg: IMessage, client: Client) => {
    logger.info(`handleClientMessage id ${msg.fromId} to ${msg.toId} messageType ${msg.messageType}`);
    if (msg.channelType === ChannelType.GROUP) {
      if (msg.messageType === MessageType.CONNECTION) {
        this.handleConnectionMessage(msg);
      } else {
        this.sendMessageToGroup(msg);
      }
    } else {
      this.sendMessageToUser(msg);
    }
  }

  private registerClient(this: Server, socket: WebSocket, id: numberOrString,
                         key: string, deviceId: string, user: any) {
    let client = this.clients[id];
    if (!client) {
      client = new Client(id, user, this);
      client.addListener("message", this.handleClientMessage);
      client.addListener("unregister", this.handleClientUnregister);
      this.clients[id] = client;
    }

    client.registerSocket(socket, key, deviceId);
    this.sockets[id] = socket;

    if (user && user.channelIds instanceof Array) {
      user.channelIds.forEach((groupId) => {
        Redis.addUserToGroup(id, groupId, (err) => {
          if (err) {
            logger.error(`registerClient addUserToGroup id ${id} groupId ${groupId} ERR ${err}`);
            return;
          }
          Redis.getUsersInsideGroup(groupId, (groupErr, userIds) => {
            if (groupErr) {
              logger.error(`registerClient getUsersInsideGroup groupId ${groupId} ERR ${groupErr}`);
              return;
            }
            States.setUsersInsideGroup(groupId, userIds);
          });
        });
      });
    }

    // tslint:disable-next-line:max-line-length
    logger.info(`REGISTERED id ${client.id} clients ${Object.keys(this.clients).length} readyState ${socket.readyState} ` +
                ` sockets ${Object.keys(this.sockets).length} wss ${this.wss.clients.size}`);
  }

  private shouldPersistForReplay(this: Server, msg: IMessage): boolean {
    if (msg.messageType === MessageType.AUDIO) { return false; }
    if (msg.messageType === MessageType.DELIVERED || msg.messageType === MessageType.READ) { return false; }
    if (msg.messageType === MessageType.CONNECTION || msg.messageType === MessageType.CONNECTION_ACK) { return false; }
    return true;
  }

  private attachServerSequence(this: Server, msg: IMessage, seq: number): IMessage {
    const enriched: IMessage = { ...msg };
    const replayStoredAt = Date.now();
    if (enriched.messageId && typeof enriched.messageId === "string") {
      try {
        const meta = JSON.parse(enriched.messageId);
        if (meta && typeof meta === "object") {
          meta.serverSeq = seq;
          meta.replayStoredAt = replayStoredAt;
          enriched.messageId = JSON.stringify(meta);
          return enriched;
        }
      } catch (e) {
        // Non-JSON messageId is valid; fallback to payload injection below.
      }
    }
    try {
      const payload = typeof enriched.payload === "string"
        ? JSON.parse(enriched.payload)
        : enriched.payload;
      if (payload && typeof payload === "object") {
        payload.serverSeq = seq;
        payload.replayStoredAt = replayStoredAt;
        enriched.payload = JSON.stringify(payload);
      }
    } catch (e) {
      // Keep original payload when not JSON.
    }
    return enriched;
  }

  private extractServerSeq(this: Server, msg: IMessage): number {
    if (msg.messageId && typeof msg.messageId === "string") {
      try {
        const meta = JSON.parse(msg.messageId);
        if (meta && typeof meta.serverSeq === "number") {
          return meta.serverSeq;
        }
      } catch (e) { /* no-op */ }
    }
    if (msg.payload && typeof msg.payload === "string") {
      try {
        const payload = JSON.parse(msg.payload);
        if (payload && typeof payload.serverSeq === "number") {
          return payload.serverSeq;
        }
      } catch (e) { /* no-op */ }
    }
    return 0;
  }

  private extractReplayStoredAt(this: Server, msg: IMessage): number {
    if (msg.messageId && typeof msg.messageId === "string") {
      try {
        const meta = JSON.parse(msg.messageId);
        if (meta && typeof meta.replayStoredAt === "number") {
          return meta.replayStoredAt;
        }
      } catch (e) { /* no-op */ }
    }
    if (msg.payload && typeof msg.payload === "string") {
      try {
        const payload = JSON.parse(msg.payload);
        if (payload && typeof payload.replayStoredAt === "number") {
          return payload.replayStoredAt;
        }
      } catch (e) { /* no-op */ }
    }
    return 0;
  }

  private shouldReplayMessage(this: Server, msg: IMessage): boolean {
    if (msg.messageType === MessageType.START_ACK ||
        msg.messageType === MessageType.START_FAILED ||
        msg.messageType === MessageType.STOP_ACK) {
      return true;
    }
    if (msg.messageType !== MessageType.TEXT) {
      return false;
    }
    if (!msg.messageId || typeof msg.messageId !== "string") {
      return false;
    }
    try {
      const meta = JSON.parse(msg.messageId);
      const textType = meta && meta.textMessageType;
      return textType === "BusyEvent" || textType === "DropCall" || textType === "CallEndedForAll";
    } catch (e) {
      return false;
    }
  }

  private replayQueuedSignals(this: Server, userId: numberOrString, lastAckedSeq: number): void {
    Redis.getSignalingOutboxEvents(userId, (err, events) => {
      if (err || !events || events.length === 0) { return; }
      const now = Date.now();
      const replay = events
        .map((item) => {
          try { return JSON.parse(item) as IMessage; } catch (e) { return null; }
        })
        .filter((msg): msg is IMessage => !!msg)
        .filter((msg) => this.extractServerSeq(msg) > lastAckedSeq)
        .filter((msg) => this.shouldReplayMessage(msg))
        .filter((msg) => {
          const storedAt = this.extractReplayStoredAt(msg);
          return storedAt > 0 && now - storedAt <= REPLAY_MAX_AGE_MS;
        })
        .sort((a, b) => this.extractServerSeq(a) - this.extractServerSeq(b));
      replay.slice(-REPLAY_MAX_EVENTS).forEach((msg) => {
        packer.pack(msg, (packErr, packed) => {
          if (packErr) { return; }
          const client = this.clients[userId];
          if (!client) { return; }
          client.send(packed);
        });
      });
    });
  }

  private getConnectionFromHeaders(headers, log: boolean = false): IConnection {
    let protocols = headers["sec-websocket-protocol"];
    if (protocols) { protocols = protocols.split(", "); }
    const token0 = protocols ? protocols[0] : null;
    const deviceId0  = protocols ? protocols[1] : null;
    const token = headers.token || headers.voicepingtoken || token0;
    const deviceId = headers.device_id || headers.deviceid || deviceId0 || token;
    const lastAckedSeq = Number(headers["x-last-acked-seq"] || 0);
    const connection = { token, deviceId, key: headers["sec-websocket-key"], lastAckedSeq };
    return connection;
  }

  private getUserFromToken(token) {
    const deferred = Q.defer();
    States.getUserFromToken(token, (err, user) => {
      if (err) {
        deferred.resolve({ uid: token });
        return;
      }
      deferred.resolve(user);
    });
    return deferred.promise;
  }

  /**
   * Websocket client verification
   *
   * @param { object } info
   * @param { function } verified
   * @private
   *
   */
  private verifyClient(this: Server, info, verified) {
    const connection = this.getConnectionFromHeaders(info.req.headers, true);
    const token = connection.token;
    if (!token) { return verified(false, 401, "Unauthorized"); }
    this.getUserFromToken(token)
      .then((user) => {
        return verified(user, 200, "Authorized");
      }).catch((err) => {
        logger.error(`verifyClient getUserFromToken ERR ${err}`);
        return verified(false, 401, "Unauthorized User");
      });
  }

  private handleWssConnection(this: Server, ws: WebSocket, req) {
    const connection = this.getConnectionFromHeaders(req.headers);
    const token = connection.token;
    if (!token) { return; }

    // If deviceId exists on redis, send duplicate login.
    this.getUserFromToken(token)
      .then((user) => {
        logger.info(`handleWssConnection after getUserFromToken. user: ${JSON.stringify(user)}`);
        const deviceId = connection.deviceId;
        const userId = user.uid;
        const key = connection.key;
        const lastAckedSeq = Number(connection.lastAckedSeq || 0);

        this.registerClient(ws, userId, key, deviceId, user);
        this.replayQueuedSignals(userId, lastAckedSeq);
      }).catch((err) => {
        logger.error(`handleWssConnection getUserFromToken ERR ${err}`);
      });

  }

  /**
   * Direct message to a destination
   *
   * @param { data } data
   * @param { number } userId
   * @private
   *
   */
  private sendDataToUser(this: Server, data: Buffer, userId: numberOrString) {
    if (this.clients.hasOwnProperty(userId)) {
      const client = this.clients[userId];
      client.send(data);
    } else {
      // debug(`sendDataToUser NOT-FOUND id ${userId}`);
    }
  }

  /**
   * Broadcast a message / data to a channel
   *
   * @param { data } data
   * @param { number } userId
   * @param { number } groupId
   * @private
   *
   */
  private sendDataFromUserToGroup(
    this: Server,
    data: Buffer, userId: numberOrString,
    groupId: numberOrString, echo: boolean = false
  ) {
    States.getUsersInsideGroup(groupId, (err, userIds) => {
      if (err) {
        logger.error(`States.getUsersInsideGroup id ${userId} groupId ${groupId} ERR ${err}`);
        return;
      }
      if (!userIds || !(userIds instanceof Array) || userIds.length <= 0) {
        logger.info(`States.getUsersInsideGroup EMPTY id ${userId} groupId ${groupId}`);
        return;
      }
      this.sendDataFromUserToSubset(data, userId, userIds, echo);
    });
  }

  private sendDataFromUserToSubset(
    this: Server,
    data: Buffer, userId: numberOrString,
    recipientIds: numberOrString[], echo: boolean = false
  ) {
    for (const recipientId of recipientIds) {
      if (!echo && recipientId.toString() === userId.toString()) { continue; }
      this.sendDataToUser(data, recipientId);
    }
  }

  /**
   *
   * Connection message handler
   *
   * @param { number } userId
   * @param { object } ws
   * @param { data } payload
   * @private
   *
   */
  private handleConnectionMessage = (msg: IMessage): void => {
    logger.info(`handleConnectionMessage id ${msg.fromId} payload ${msg.payload}`);
    /* Buffer should be device token for voip push notification
       device token needs to be either registered or unregistered
       when a new device is connected */
    if (msg.payload) {
      const deviceToken = msg.payload.toString();
      Redis.getGroupsOfUser(msg.fromId, (err, groupIds) => {
        logger.info(`Redis.getGroupsOfUser id ${msg.fromId} groupIds ${groupIds}`);
      });
    }
  }
}

module.exports = Server;
