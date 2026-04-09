import * as redis from "redis";

import config = require("./config");
import logger = require("./logger");
import { IMessage, numberOrString } from "./types";

const USER_INSTANCE_KEY_PREFIX = "u";
const USER_INSTANCE_KEY_SUFFIX = "i";
const INSTANCE_CHANNEL_KEY_PREFIX = "i";
const INSTANCE_CHANNEL_KEY_SUFFIX = "c";
const PRESENCE_TTL = config.instance.presenceTtl;

/**
 * Creates a Redis client using the configured host/port/password.
 */
function createRedisClient() {
  return redis.createClient(config.redis.port, config.redis.host, {
    auth_pass: config.redis.password
  });
}

/**
 * Builds the Redis key used to store the user -> instance mapping.
 */
function userInstanceKey(userId: numberOrString): string {
  return `${USER_INSTANCE_KEY_PREFIX}.${userId}.${USER_INSTANCE_KEY_SUFFIX}`;
}

/**
 * Builds the Redis Pub/Sub channel key for a specific instance.
 */
function instanceChannelKey(instanceId: string): string {
  return `${INSTANCE_CHANNEL_KEY_PREFIX}.${instanceId}.${INSTANCE_CHANNEL_KEY_SUFFIX}`;
}

const client = createRedisClient();
const publisher = createRedisClient();
const subscriber = createRedisClient();

[client, publisher, subscriber].forEach((redisClient) => {
  redisClient.on("error", function(err) {
    logger.error("Distributed Redis client.on.error: ", err);
  });
});

let subscribedChannel: string;
let subscribedHandler: (userId: numberOrString, message: IMessage) => void;

subscriber.on("message", (channel: string, payload: string) => {
  if (!subscribedHandler || !subscribedChannel || channel !== subscribedChannel) { return; }

  try {
    const parsed = JSON.parse(payload);
    if (!parsed || !parsed.hasOwnProperty("userId") || !parsed.message) { return; }

    subscribedHandler(parsed.userId, parsed.message);
  } catch (err) {
    logger.error(`Distributed subscriber JSON.parse ERR ${err}`);
  }
});

class Distributed {

  /**
   * Stores/refreshes which instance currently owns a user connection.
   * The mapping is written with TTL so stale entries expire automatically.
   */
  public static setUserInstance(
    userId: numberOrString,
    instanceId: string,
    callback?: (err: Error, succeed: boolean) => void
  ) {
    client.setex(userInstanceKey(userId), PRESENCE_TTL, instanceId, function(err, reply) {
      if (!callback) { return; }
      if (err) { return callback(err, false); }
      return callback(null, reply === "OK");
    });
  }

  /**
   * Fetches the instance currently mapped to a user.
   */
  public static getUserInstance(
    userId: numberOrString,
    callback: (err: Error, instanceId: string) => void
  ) {
    client.get(userInstanceKey(userId), function(err, instanceId) {
      if (err) { return callback(err, null); }
      return callback(null, instanceId);
    });
  }

  /**
   * Clears the user -> instance mapping only if it still points
   * to the provided instanceId (prevents deleting newer ownership).
   */
  public static clearUserInstance(
    userId: numberOrString,
    instanceId: string,
    callback?: (err: Error, succeed: boolean) => void
  ) {
    client.get(userInstanceKey(userId), function(err, currentInstanceId) {
      if (err) {
        if (callback) { return callback(err, false); }
        return;
      }

      if (!currentInstanceId || currentInstanceId !== instanceId) {
        if (callback) { return callback(null, false); }
        return;
      }

      client.del(userInstanceKey(userId), function(err1) {
        if (callback) {
          if (err1) { return callback(err1, false); }
          return callback(null, true);
        }
      });
    });
  }

  /**
   * Publishes a direct message event to the target instance channel.
   */
  public static publishMessageToInstance(
    instanceId: string,
    userId: numberOrString,
    message: IMessage,
    callback?: (err: Error, receivers: number) => void
  ) {
    const payload = JSON.stringify({ userId, message });
    publisher.publish(instanceChannelKey(instanceId), payload, function(err, receivers) {
      if (!callback) { return; }
      if (err) { return callback(err, 0); }
      return callback(null, receivers);
    });
  }

  /**
   * Subscribes this process to its instance channel and installs
   * the handler used to deliver forwarded user messages locally.
   */
  public static subscribeToInstance(
    instanceId: string,
    handler: (userId: numberOrString, message: IMessage) => void
  ) {
    const channel = instanceChannelKey(instanceId);
    subscribedHandler = handler;

    if (subscribedChannel === channel) { return; }

    if (subscribedChannel) {
      subscriber.unsubscribe(subscribedChannel);
    }

    subscribedChannel = channel;
    subscriber.subscribe(channel);
  }
}

export = Distributed;
