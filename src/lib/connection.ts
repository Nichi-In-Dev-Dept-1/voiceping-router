import * as cluster from "cluster";
import * as EventEmitter from "events";

import * as dbug from "debug";
import * as WebSocket from "ws";

import config = require("./config");
import logger = require("./logger");
import MessageType = require("./messagetype");
import { packer } from "./packer";
import { IMessage, numberOrString } from "./types";

const dbug1 = dbug("vp:connection");
function debug(msg: string) {
  dbug1((cluster.worker ? `worker ${cluster.worker.id} ` : "") + msg);
}

export default class Connection extends EventEmitter {
  public deviceId: string;
  public key: string;

  private clientId: numberOrString;
  private heartbeatCloseTimer: NodeJS.Timer;
  private pongTimer: NodeJS.Timer;
  private socket: WebSocket;
  private terminated: boolean = false;
  private timestamp: number;

  constructor(key: string, socket: WebSocket, deviceId: string, clientId: numberOrString) {
    super();

    this.clientId = clientId;
    this.deviceId = deviceId;
    this.key = key;
    this.socket = socket;
    this.timestamp = Date.now();

    // Enable TCP keepalive on the raw socket so the kernel surfaces a dead
    // peer (NAT eviction / cellular handoff / firewall RST) within tens of
    // seconds even if the application ping is somehow delayed. Defence in
    // depth alongside the pong-timeout logic below.
    const rawSocket = (socket as any)._socket;
    if (rawSocket && typeof rawSocket.setKeepAlive === "function") {
      try {
        rawSocket.setKeepAlive(true, config.tcpKeepAliveDelay);
      } catch (exception) {
        debug(`id ${this.clientId} key ${this.key}` +
              ` setKeepAlive ERR ${JSON.stringify(exception)}` +
              ` device ${this.deviceId}`);
      }
    }

    socket.addListener("close", this.handleSocketClose);
    socket.addListener("error", this.handleSocketError);
    socket.addListener("message", this.handleSocketMessage);
    socket.addListener("ping", this.handleSocketPing);
    socket.addListener("pong", this.handleSocketPong);
  }

  public ping(this: Connection) {
    if (this.socket.readyState !== WebSocket.OPEN) { return; }
    try {
      this.socket.ping("voiceping:" + this.clientId, false);
    } catch (exception) {
      debug(`id ${this.clientId} key ${this.key}` +
            ` PING ERR ${JSON.stringify(exception)}` +
            ` device ${this.deviceId}`);
      return;
    }

    // Start (or refresh) a deadline for the matching pong. Any inbound frame
    // — pong, message, or client-initiated ping — clears it. If nothing
    // arrives in time the socket is treated as dead and terminated, which
    // emits "close" and propagates cleanup to Client/Server, removing the
    // stale entry from this.clients[userId] within ~pingInterval+pongTimeout.
    this.armPongTimer();
  }

  public getLastSeenAt(this: Connection) {
    return this.timestamp;
  }

  public isOpen(this: Connection) {
    return this.socket.readyState === WebSocket.OPEN;
  }

  public terminate(this: Connection) {
    logger.info(`id: ${this.clientId} key: ${this.key} TERMINATE readyState: ${this.socket.readyState}`);
    try {
      this.socket.terminate();
    } catch (exception) {
      debug(`id ${this.clientId} key ${this.key} TERMINATE ERR ${JSON.stringify(exception)} device ${this.deviceId}`);
    }
  }

  public closeDueToHeartbeatTimeout(this: Connection, idleTime: number) {
    logger.info(
      `id: ${this.clientId} key: ${this.key} HEARTBEAT_TIMEOUT idleTime: ${idleTime}` +
      ` readyState: ${this.socket.readyState}`
    );

    if (this.socket.readyState !== WebSocket.OPEN) {
      this.terminate();
      return;
    }

    try {
      this.socket.close(1001, "heartbeat timeout");
    } catch (exception) {
      debug(`id ${this.clientId} key ${this.key}` +
            ` HEARTBEAT CLOSE ERR ${JSON.stringify(exception)}` +
            ` device ${this.deviceId}`);
      this.terminate();
      return;
    }

    this.clearHeartbeatCloseTimer();
    this.heartbeatCloseTimer = setTimeout(() => {
      if (this.socket.readyState !== WebSocket.CLOSED) {
        this.terminate();
      }
    }, config.heartbeatCloseGracePeriod);
  }

  public send(this: Connection, data: Buffer, msg?: IMessage) {
    if (msg && (msg.messageType === MessageType.LOGIN_DUPLICATED || msg.messageType === MessageType.CONNECTION_ACK)) {
      debug(`id ${this.clientId} SEND readyState: ${this.socket.readyState}, msg: ${JSON.stringify(msg)}`);
    }
    if (this.socket.readyState !== WebSocket.OPEN) {
      // Server believes this connection is still routable (it is in
      // Client.connections), but the socket has already moved out of OPEN.
      // Make the silent drop visible so the "calls not received" symptom
      // can be diagnosed in production.
      logger.info(`id ${this.clientId} key ${this.key} SEND_SKIPPED readyState ${this.socket.readyState}` +
                  ` messageType ${msg ? msg.messageType : "?"} device ${this.deviceId}`);
      return;
    }
    try {
      this.socket.send(data, (err) => {
        if (!err) { return; }
        logger.error(`id ${this.clientId} key ${this.key} SEND_FAILED ${err.message || err}` +
                     ` messageType ${msg ? msg.messageType : "?"} device ${this.deviceId}`);
        // Write failure on an OPEN socket means the underlying transport is
        // dead. Schedule termination on the next tick so we don't re-enter
        // the ws event loop synchronously; handleSocketClose will then
        // propagate cleanup through Client/Server.
        if (this.terminated) { return; }
        this.terminated = true;
        setImmediate(() => this.terminate());
      });
    } catch (exception) {
      logger.error(`id ${this.clientId} key ${this.key}` +
                   ` SEND ERR ${JSON.stringify(exception)} device ${this.deviceId}`);
      if (!this.terminated) {
        this.terminated = true;
        setImmediate(() => this.terminate());
      }
    }
  }

  public message(this: Connection, msg: IMessage) {
    debug(`id ${this.clientId} SEND_MESSAGE ${JSON.stringify(msg)}`);
    packer.pack(msg, (err, packed) => {
      if (err) {
        debug(`id ${this.clientId} key ${this.key}` +
              ` PACK ERR ${err} ${JSON.stringify(msg)}` +
              ` device ${this.deviceId}`);
        return;
      }
      this.send(packed, msg);
    });
  }

  public close(this: Connection) {
    logger.info(`id: ${this.clientId} key: ${this.key} BEFORE CLOSE readyState: ${this.socket.readyState}`);
    this.socket.close();
  }

  // (WEB)SOCKET (WS) EVENT HANDLERS

  private handleSocketClose = (code, reason) => {
    debug(`id ${this.clientId} key ${this.key}` +
          ` handleSocketClose code ${code} reason ${reason}` +
          ` device ${this.deviceId}`);

    this.clearHeartbeatCloseTimer();
    this.clearPongTimer();

    this.socket.removeListener("close", this.handleSocketClose);
    this.socket.removeListener("error", this.handleSocketError);
    this.socket.removeListener("message", this.handleSocketMessage);
    this.socket.removeListener("ping", this.handleSocketPing);
    this.socket.removeListener("pong", this.handleSocketPong);

    this.emit("close", this);
  }

  private handleSocketError = (reason, code) => {
    debug(`id ${this.clientId} key ${this.key}` +
          ` handleSocketError code ${code} reason ${reason}` +
          ` device ${this.deviceId}`);
  }

  private handleSocketMessage = (data: Buffer) => {
    this.timestamp = Date.now();
    // Any inbound frame proves liveness — clear the pending pong deadline.
    this.clearPongTimer();
    debug(`*************************************`);
    debug(`id ${this.clientId} key ${this.key}` +
              ` handleSocketMessage RAW data: ${data.toString()}` +
              ` device ${this.deviceId}` +
              ` socketState ${this.socket.readyState}`);
    packer.unpack(data, (err: Error, msg: IMessage) => {
      if (err) {
        debug(`id ${this.clientId} key ${this.key}` +
              ` UNPACK ERR ${err} ${JSON.stringify(msg)}` +
              ` device ${this.deviceId}`);
        return;
      }
      debug(`id ${this.clientId} key ${this.key}` +
          ` handleSocketMessage device ${this.deviceId}` +
          ` socketState ${this.socket.readyState}` +
          ` msg: ${JSON.stringify(msg)}`);

      this.emit("message", msg);
    });
  }

  private handleSocketPing = (data: Buffer) => {
    this.timestamp = Date.now();
    this.clearPongTimer();
    let payload;
    if (data instanceof Buffer) { payload = data.toString(); }
    debug(`id ${this.clientId} key ${this.key}` +
          ` handleSocketPing ${payload}` +
          ` device ${this.deviceId}`);
  }

  private handleSocketPong = (data: Buffer) => {
    this.timestamp = Date.now();
    this.clearPongTimer();
    let payload;
    if (data instanceof Buffer) { payload = data.toString(); }
    debug(`id ${this.clientId} key ${this.key}` +
          ` handleSocketPong ${payload}` +
          ` device ${this.deviceId}`);
    this.emit("pong", payload);
  }

  private clearHeartbeatCloseTimer(this: Connection) {
    if (!this.heartbeatCloseTimer) { return; }
    clearTimeout(this.heartbeatCloseTimer);
    this.heartbeatCloseTimer = null;
  }

  private armPongTimer(this: Connection) {
    this.clearPongTimer();
    if (!config.pongTimeout || config.pongTimeout <= 0) { return; }
    this.pongTimer = setTimeout(() => {
      this.pongTimer = null;
      if (this.terminated) { return; }
      const idleTime = Date.now() - this.timestamp;
      logger.info(
        `id: ${this.clientId} key: ${this.key} PONG_TIMEOUT` +
        ` idleTime: ${idleTime} readyState: ${this.socket.readyState}`
      );
      this.terminated = true;
      this.terminate();
    }, config.pongTimeout);
  }

  private clearPongTimer(this: Connection) {
    if (!this.pongTimer) { return; }
    clearTimeout(this.pongTimer);
    this.pongTimer = null;
  }
}
