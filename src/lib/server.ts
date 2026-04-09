import * as cluster from "cluster";

import * as dbug from "debug";
import * as _ from "lodash";
import * as Q from "q";
import * as WebSocket from "ws";

import ChannelType = require("./channeltype");
import Client, { IClients } from "./client";
import config = require("./config");
import Distributed = require("./distributed");

import logger = require("./logger");
import MessageType = require("./messagetype");
import { packer } from "./packer";
import Redis = require("./redis");
import States from "./states";
import { IMessage, numberOrString } from "./types";

const WORKER_NUMBER = cluster.worker ? cluster.worker.id : "-";
const dbug1 = dbug("vp:router");
function debug(msg: string) {
  dbug1((cluster.worker ? `worker ${cluster.worker.id} ` : "") + msg);
}

export interface IServer {
  sendMessageToUser: (mesage: IMessage) => void;
  sendMessageToGroup: (message: IMessage) => void;
}
interface IConnection {
  token: string;
  deviceId: string;
  key: string;
}

// class Server implements IServer {
class Server implements IServer {

  private clients: IClients = {};
  private sockets = {};
  private deviceTokens = {};
  private instanceHeartbeat: NodeJS.Timer = null;
  private instanceId: string = config.instance.id;
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

    Distributed.subscribeToInstance(this.instanceId, this.handleDistributedUserMessage);
    this.periodicRefreshUserInstances();
  }

  // IServer Implementation

  public sendMessageToUser(this: Server, msg: IMessage) {
    packer.pack(msg, (err, packed) => {
      this.sendPackedDataToUser(packed, msg.toId, msg);
    });
  }

  public sendMessageToGroup(this: Server, msg: IMessage) {
    logger.info(`sendMessageToGroup from: ${msg.fromId} to: ${msg.toId} messageType: ${msg.messageType}`);
    packer.pack(msg, (err, packed) => {
      this.sendDataFromUserToGroup(packed, msg.fromId, msg.toId, false, msg);
    });
  }

  private handleClientUnregister = (client: Client) => {
    const clientId = client.id;
    if (!this.clients[clientId]) { return; }

    client.removeListener("message", this.handleClientMessage);
    client.removeListener("unregister", this.handleClientUnregister);
    delete this.clients[clientId];
    delete this.sockets[clientId];
    Distributed.clearUserInstance(clientId, this.instanceId, (err, cleared) => {
      if (err) {
        logger.error(`Distributed.clearUserInstance id ${clientId} instance ${this.instanceId} ERR ${err}`);
      } else if (cleared) {
        debug(`Distributed.clearUserInstance id ${clientId} instance ${this.instanceId}`);
      }
    });
    logger.info(`UNREGISTERED id ${clientId} clients ${Object.keys(this.clients).length}` +
                ` sockets ${Object.keys(this.sockets).length} wss ${this.wss.clients.size}`);
  }

  private handleClientMessage = (msg: IMessage, client: Client) => {
    logger.info(`handleClientMessage id ${msg.fromId} to ${msg.toId} messageType ${msg.messageType}`);
    if (msg.channelType === ChannelType.GROUP) {
      if (msg.messageType === MessageType.CONNECTION) {
        this.handleConnectionMessage(msg);
      } else if (msg.messageType === MessageType.USER_REMOVE_ALL) {
        this.removeFromAllGroups(msg.fromId);
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
    this.refreshUserInstance(id);

    // tslint:disable-next-line:max-line-length
    logger.info(`REGISTERED id ${client.id} clients ${Object.keys(this.clients).length} readyState ${socket.readyState} ` +
                ` sockets ${Object.keys(this.sockets).length} wss ${this.wss.clients.size}`);
  }

  private getConnectionFromHeaders(headers, log: boolean = false): IConnection {
    let protocols = headers["sec-websocket-protocol"];
    if (protocols) { protocols = protocols.split(", "); }
    const token0 = protocols ? protocols[0] : null;
    const deviceId0  = protocols ? protocols[1] : null;
    const token = headers.token || headers.voicepingtoken || token0;
    const deviceId = headers.device_id || headers.deviceid || deviceId0 || token;
    const connection = { token, deviceId, key: headers["sec-websocket-key"] };
    return connection;
  }

  private getUserFromToken(token) {
    const deferred = Q.defer();
    deferred.resolve({ uid: token });
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

        this.registerClient(ws, userId, key, deviceId, user);
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
  private sendDataToUser(this: Server, data: Buffer, userId: numberOrString): boolean {
    if (!this.clients.hasOwnProperty(userId)) { return false; }

    const client = this.clients[userId];
    client.send(data);
    return true;
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
    groupId: numberOrString, echo: boolean = false,
    msg?: IMessage
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
      for (const recipientId of userIds) {
        if (!echo && recipientId.toString() === userId.toString()) { continue; }
        this.sendPackedDataToUser(data, recipientId, msg);
      }
    });
  }

  private sendPackedDataToUser(this: Server, data: Buffer, userId: numberOrString, msg?: IMessage) {
    if (this.sendDataToUser(data, userId)) { return; }

    if (msg) {
      this.routeMessageToRemoteUser(userId, msg);
      return;
    }

    debug(`sendPackedDataToUser NOT-FOUND id ${userId}`);
  }

  private routeMessageToRemoteUser(this: Server, userId: numberOrString, msg: IMessage) {
    Distributed.getUserInstance(userId, (err, instanceId) => {
      if (err) {
        logger.error(`Distributed.getUserInstance id ${userId} ERR ${err}`);
        return;
      }

      if (!instanceId) {
        if (msg.messageType === MessageType.AUDIO) {
          debug(`sendMessageToUser type AUDIO NOT-FOUND id ${userId} ${JSON.stringify(msg)}`);
        } else {
          debug(`sendMessageToUser type NON-AUDIO NOT-FOUND id ${userId} ${JSON.stringify(msg)}`);
        }
        return;
      }

      if (instanceId === this.instanceId) {
        debug(`Distributed.getUserInstance id ${userId} instance ${instanceId} local-instance-no-client`);
        return;
      }

      Distributed.publishMessageToInstance(instanceId, userId, msg, (publishErr, receivers) => {
        if (publishErr) {
          logger.error(`Distributed.publishMessageToInstance user ${userId} instance ${instanceId} ERR ${publishErr}`);
          return;
        }

        debug(`Distributed.publishMessageToInstance user ${userId} instance ${instanceId} receivers ${receivers}`);
      });
    });
  }

  private handleDistributedUserMessage = (userId: numberOrString, msg: IMessage) => {
    packer.pack(msg, (err, packed) => {
      if (err) {
        logger.error(`handleDistributedUserMessage pack ERR ${err}`);
        return;
      }

      if (!this.sendDataToUser(packed, userId)) {
        debug(`handleDistributedUserMessage NOT-FOUND id ${userId} ${JSON.stringify(msg)}`);
      }
    });
  }

  private refreshUserInstance(this: Server, userId: numberOrString) {
    Distributed.setUserInstance(userId, this.instanceId, (err, succeed) => {
      if (err) {
        logger.error(`Distributed.setUserInstance id ${userId} instance ${this.instanceId} ERR ${err}`);
      } else if (!succeed) {
        debug(`Distributed.setUserInstance id ${userId} instance ${this.instanceId} failed`);
      }
    });
  }

  private periodicRefreshUserInstances(this: Server) {
    if (this.instanceHeartbeat || config.instance.heartbeatInterval <= 0) { return; }

    this.instanceHeartbeat = setInterval(() => {
      Object.keys(this.clients).forEach((clientId) => {
        this.refreshUserInstance(clientId);
      });
    }, config.instance.heartbeatInterval);
  }

  private removeFromAllGroups(this: Server, userId: numberOrString): void {
    Redis.getGroupsOfUser(userId, (err, groupIds) => {
      if (err) {
        logger.error(`Redis.getGroupsOfUser id ${userId} ERR ${err}`);
        return;
      }

      const groups = groupIds || [];
      Redis.removeUserFromAllGroups(userId, (removeErr) => {
        if (removeErr) {
          logger.error(`Redis.removeUserFromAllGroups id ${userId} ERR ${removeErr}`);
          return;
        }

        groups.forEach((groupId) => {
          Redis.getUsersInsideGroup(groupId, (groupErr, userIds) => {
            if (groupErr) {
              logger.error(`Redis.getUsersInsideGroup groupId ${groupId} ERR ${groupErr}`);
              return;
            }
            States.setUsersInsideGroup(groupId, userIds);
          });
        });
      });
    });
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
