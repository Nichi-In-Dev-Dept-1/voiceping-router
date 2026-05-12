import * as redis from "redis";
import config = require("./config");

import { Keys } from "./keys";
import logger = require("./logger");
import { numberOrString } from "./types";

const client = redis.createClient(config.redis.port, config.redis.host, {
  auth_pass: config.redis.password
});

client.on("error", function(err) {
  logger.error("Redis: client.on.error: ", err);
});

const CLEAN_INTERVAL = config.redis.cleanInterval;
const CLEAN_GROUPS_AMOUNT = config.redis.cleanGroupsAmount;
const CLEAN_LOG_ENABLED = config.redis.cleanLogEnabled;
const DRY_CLEAN_ENABLED = config.redis.dryCleanEnabled;

let cleanInterval: NodeJS.Timer;
let cleanGroup: number = 1;

// TTL for active-call Redis entries — auto-expires stale entries after busyTimeout seconds.
// Match the private/group floor TTL so ac keys never outlive their floor lock
const ACTIVE_STATE_TTL = Math.ceil((config.group.busyTimeout / 1000) + 30);
const SIGNALING_OUTBOX_MAX_ITEMS = 200;
// 15 s is long enough to prevent duplicate processing of a retried START,
// but short enough that a failed attempt (Redis error, floor-acquire error)
// doesn't permanently block the next retry for 2 minutes.
const SIGNALING_OPERATION_TTL_SEC = 15;

class Redis {
  public static nextSignalingSeq(
    userId: numberOrString,
    callback: (err: Error, seq: number) => void
  ): void {
    client.incr(Keys.forSignalingSeq(userId), (err, seq) => {
      if (err) { return callback(err, 0); }
      return callback(null, Number(seq || 0));
    });
  }

  public static pushSignalingOutboxEvent(
    userId: numberOrString,
    serializedEvent: string,
    callback?: (err: Error, succeed: boolean) => void
  ): void {
    const key = Keys.forSignalingOutbox(userId);
    const multi = client.multi();
    multi.lpush(key, serializedEvent);
    multi.ltrim(key, 0, SIGNALING_OUTBOX_MAX_ITEMS - 1);
    multi.exec((err) => {
      if (callback) { return callback(err || null, !err); }
    });
  }

  public static getSignalingOutboxEvents(
    userId: numberOrString,
    callback: (err: Error, events: string[]) => void
  ): void {
    client.lrange(Keys.forSignalingOutbox(userId), 0, SIGNALING_OUTBOX_MAX_ITEMS - 1, (err, events) => {
      if (err) { return callback(err, []); }
      return callback(null, events || []);
    });
  }

  public static reserveOperation(
    operationId: string,
    ttlSec: number = SIGNALING_OPERATION_TTL_SEC,
    callback?: (err: Error, reserved: boolean) => void
  ): void {
    const key = Keys.forSignalingOperation(operationId);
    (client as any).set(key, "1", "NX", "EX", ttlSec, (err: Error, reply: string) => {
      if (callback) { return callback(err || null, reply === "OK"); }
    });
  }

  public static incrementRegisterDevicesCount(callback: (err: Error, registerDevicesCount: number) => void) {
    client.incr(Keys.forRegisterDevicesCount, function(err, registerDevicesCount) {
      return callback(err, registerDevicesCount);
    });
  }

  public static incrementGroupsPushCount(callback: (err: Error, groupsPushCount: number) => void) {
    client.incr(Keys.forGroupsPushCount, function(err, groupsPushCount) {
      return callback(err, groupsPushCount);
    });
  }

  public static incrementUsersPushCount(callback: (err: Error, usersPushCount: number) => void) {
    client.incr(Keys.forUsersPushCount, function(err, usersPushCount) {
      return callback(err, usersPushCount);
    });
  }

  public static getUserWithToken(
    token: string,
    callback: (err: Error, user: any) => void) {

    client.hget(Keys.forUUIDs(), token, function(err, user) {
      if (err) { return callback(err, null); }
      return callback(null, JSON.parse(user));
    });
  }

  public static getDeviceTokensOfUsers(userIds: numberOrString[],
                                       callback: (err: Error, deviceTokens: string[]) => void) {
    const multi = client.multi();
    userIds.forEach(function(userId) {
      multi.hget(Keys.forUser(userId), "deviceToken");
    });

    multi.exec(function(err, replies) {
      if (err) { return callback(err, null); }
      return callback(null, replies);
    });
  }

  public static getDeviceTokenOfUser(userId: numberOrString,
                                     callback: (err: Error, deviceToken: string) => void) {
    client.hget(Keys.forUser(userId), "deviceToken", function(err, deviceToken) {
      if (err) { return callback(err, null); }
      return callback(null, deviceToken);
    });
  }

  public static setDeviceTokenOfUser(
    userId: numberOrString, deviceToken: string,
    callback: (err: Error, succeed: boolean) => void
  ) {
    client.hset(Keys.forUser(userId), "deviceToken", deviceToken, function(err, reply) {
      if (err) { return callback(err, null); }
      return callback(null, true);
    });
  }

  public static removeDeviceTokenOfUser(userId: numberOrString,
                                        callback: (err: Error, succeed: boolean) => void) {

    client.hdel(Keys.forUser(userId), "deviceToken", function(err, reply) {
      if (err) { return callback(err, false); }
      return callback(null, true);
    });
  }

  public static getUserNameOfUser(userId: numberOrString,
                                  callback: (err: Error, userName: string) => void) {
    client.hget(Keys.forUser(userId), "userName", function(err, userName) {
      if (err) { return callback(err, null); }
      return callback(null, userName);
    });
  }

  public static setUserNameOfUser(
    userId: numberOrString, userName: string, userUuid: string,
    callback: (err: Error, succeed: boolean) => void) {

    client.hmset(
      Keys.forUser(userId),
      "userName", userName,
      "lastSeen", Date.now(),
      function(err1, deviceId1) {

        if (err1) { return callback(err1, false); }
        return callback(null, true);
      });
  }

  public static getDeviceIdOfUser(userId: numberOrString,
                                  callback: (err: Error, deviceId: string) => void) {
    client.hget(Keys.forUser(userId), "deviceId", function(err, deviceId) {
      if (err) { return callback(err, null); }
      return callback(null, deviceId);
    });
  }

  public static setDeviceIdOfUser(
    userId: numberOrString, deviceId: string,
    callback?: (err: Error, succeed: boolean) => void) {

    client.hmset(
      Keys.forUser(userId),
      "deviceId", deviceId,

      function(err1, deviceId1) {

        if (err1) {
          if (callback) { return callback(err1, false); }
        }
        if (callback) { return callback(null, true); }
      });
  }

  public static getLastSeenOfUser(userId: numberOrString,
                                  callback: (err: Error, lastSeen: number) => void) {
    client.hget(Keys.forUser(userId), "lastSeen", function(err, lastSeen) {
      if (err) { return callback(err, 0); }
      return callback(null, Number(lastSeen));
    });
  }

  public static getStatusOfUser(userId: numberOrString,
                                callback: (err: Error, status: string) => void) {
    client.hget(Keys.forUser(userId), "status", function(err, status) {
      if (err) { return callback(err, null); }
      return callback(null, status);
    });
  }

  public static setStatusOfUser(
    userId: numberOrString, status: string,
    callback: (err: Error, status: string) => void) {

    client.hset(Keys.forUser(userId), "status", status, function(err1, status1) {
      if (err1) { return callback(err1, null); }
      return callback(null, status);
    });
  }

  public static setGroupsOfUser(
    userId: numberOrString, groupIds: number[],
    callback: (err: Error, succeed: boolean) => void) {

    client.del(Keys.forGroupsOfUser(userId), function(err, reply) {
      if (!groupIds || groupIds.length === 0) {
        return callback(null, true);
      }
      client.sadd(Keys.forGroupsOfUser(userId), groupIds, function(err1, reply1) {
        if (err1) { return callback(err1, false); }

        const multi = client.multi();
        groupIds.forEach(function(groupId) {
          multi.sadd(Keys.forUsersInsideGroup(groupId), userId);
        });

        multi.exec(function(err2, replies2) {
          if (err2) { return callback(err2, null); }
          return callback(null, true);
        });
        return callback(null, true);
      });
    });
  }

  public static getGroupsOfUser(
    userId: numberOrString,
    callback: (err: Error, groupIds: number[]) => void) {

    client.smembers(Keys.forGroupsOfUser(userId), function(err, groupIds) {
      if (err) { return callback(err, null); }
      return callback(null, groupIds);
    });
  }

  public static setGroup(
    groupId: numberOrString, name: string,
    callback?: (err: Error, succeed: boolean) => void) {

    client.hmset(
      Keys.forGroup(groupId),
      "name", name,
      "lastUpdated", Date.now(),
      function(err1, deviceId1) {
        if (err1) { return callback(err1, false); }
        return callback(null, true);
      }
    );
  }

  public static getLastUpdatedOfGroup(groupId: numberOrString,
                                      callback: (err: Error, status: string) => void) {
    client.hget(Keys.forGroup(groupId), "lastUpdated", function(err, status) {
      if (err) { return callback(err, null); }
      return callback(null, status);
    });
  }

  public static setUsersInsideGroup(
    groupId: numberOrString, userIds: number[],
    callback: (err: Error, succeed: boolean) => void) {

    client.del(Keys.forUsersInsideGroup(groupId), function(err, reply) {
      if (!userIds || userIds.length === 0) {
        return callback(null, true);
      }
      client.sadd(Keys.forUsersInsideGroup(groupId), userIds, function(err1, reply1) {
        if (err1) { return callback(err1, false); }

        const multi = client.multi();
        userIds.forEach(function(userId) {
          multi.sadd(Keys.forGroupsOfUser(userId), groupId);
        });

        multi.exec(function(err2, replies2) {
          if (err2) { return callback(err2, null); }
          return callback(null, true);
        });
      });
    });
  }

  public static getUsersInsideGroup(
    groupId: numberOrString,
    callback: (err: Error, userIds: number[]) => void) {

    client.smembers(Keys.forUsersInsideGroup(groupId), function(err, userIds) {
      if (err) { return callback(err, null); }
      return callback(null, userIds);
    });
  }

  public static addUserToGroup(
    userId: numberOrString, groupId: numberOrString,
    callback: (err: Error, succeed: boolean) => void) {

    const multi = client.multi();
    multi.sadd(Keys.forGroupsOfUser(userId), groupId);
    multi.sadd(Keys.forUsersInsideGroup(groupId), userId);
    multi.exec(function(err, replies) {
      if (err) { return callback(err, null); }
      return callback(null, !!replies);
    });
  }

  public static removeUserFromGroup(
    userId: numberOrString, groupId: numberOrString,
    callback: (err: Error, succeed: boolean) => void) {

    const multi = client.multi();
    multi.srem(Keys.forGroupsOfUser(userId), groupId);
    multi.srem(Keys.forUsersInsideGroup(groupId), userId);
    multi.exec(function(err, replies) {
      if (err) { return callback(err, null); }
      return callback(null, !!replies);
    });
  }

  public static removeUserFromAllGroups(
    userId: numberOrString,
    callback: (err: Error, succeed: boolean) => void) {

    Redis.getGroupsOfUser(userId, (err, groupIds) => {
      if (err) { return callback(err, null); }
      if (!groupIds || groupIds.length === 0) { return callback(null, true); }

      const multi = client.multi();
      multi.del(Keys.forGroupsOfUser(userId));
      groupIds.forEach((groupId) => {
        multi.srem(Keys.forUsersInsideGroup(groupId), userId);
      });
      multi.exec(function(transactionErr, replies) {
        if (transactionErr) { return callback(transactionErr, null); }
        return callback(null, !!replies);
      });
    });
  }

  public static addMessageToGroup(messageId: string, groupId: numberOrString,
                                  callback: (err: Error, succeed: boolean) => void) {
    const multi = client.multi();
    const key = Keys.forMessagesOfGroup(groupId);
    multi.lpush(key, messageId);
    multi.exec(function(err, reply) {
      if (err) { return callback(err, false); }
      return callback(null, true);
    });
  }

  public static addMessageToUser(messageId: string, userId: numberOrString,
                                 callback: (err: Error, succeed: boolean) => void) {
    const multi = client.multi();
    const key = Keys.forMessagesOfUser(userId);
    logger.info(`Redis addMessageToUser key: ${key}`);
    multi.lpush(key, messageId);
    multi.exec(function(err, reply) {
      logger.info(`Redis addMessageToUser RESULT key: ${key}, reply: ${JSON.stringify(reply)}`);
      if (err) { return callback(err, false); }
      return callback(null, true);
    });
  }

  public static getMessagesOfUser(userId: numberOrString, callback: (err: Error, messageIds: string[]) => void) {
    logger.info(`Redis getMessagesOfUser key: ${Keys.forMessagesOfUser(userId)}`);
    client.lrange(Keys.forMessagesOfUser(userId), 0, 9, function(err, messageIds) {
      // tslint:disable-next-line:max-line-length
      logger.info(`Redis getMessagesOfUser RESULT key: ${Keys.forMessagesOfUser(userId)}, reply: ${JSON.stringify(messageIds)}`);
      if (err) { return callback(err, null); }
      return callback(null, messageIds);
    });
  }

  public static removeMessagesFromUser(messages: string[], userId: numberOrString,
                                       callback: (err: Error, succeed: boolean) => void) {
    const multi = client.multi();
    if (messages && messages.length > 0) {
      messages.forEach(function(message) {
        multi.lrem(Keys.forMessagesOfUser(userId), 0, message);
      });
    }

    multi.exec(function(err, replies) {
      if (err) { return callback(err, false); }
      return callback(null, true);
    });
  }

  public static getBusyStateOfGroup(
    groupId: numberOrString,
    callback: (err: Error, busyWithUserId: numberOrString) => void
  ) {
    client.hget(Keys.forCurrentMessageOfGroup(groupId), function(err, busyWithUserId) {
      if (err) { return callback(err, null); }
      return callback(null, busyWithUserId);
    });
  }

  public static setBusyStateOfGroup(
    groupId: numberOrString, busyWithUserId: numberOrString,
    callback: (err: Error, busyWithUserId: numberOrString) => void) {

    Redis.getBusyStateOfGroup(groupId, function(err1, busyWithUserId1) {
      if (!busyWithUserId1 || busyWithUserId1 !== busyWithUserId) {
        client.hset(Keys.forCurrentMessageOfGroup(groupId), "fromId", busyWithUserId, function(err2, busyWithUserId2) {
          if (err2) { return callback(err2, null); }
          return callback(null, busyWithUserId);
        });
      } else {
        return callback(null, busyWithUserId);
      }
    });
  }

  public static removeKeyUsersInsideGroup(
    groupId: numberOrString, callback: (err: Error, succeed: boolean) => void) {

    client.del(Keys.forUsersInsideGroup(groupId), (err, reply) => {
      if (err) { return callback(err, false); }
      return callback(null, true);
    });
  }

  // Active call state — persisted with TTL so stale entries auto-expire after busyTimeout.
  // These mirror the in-memory groupsActiveParticipantsSet / activeCallGroupsOfUsersSet
  // in states.ts, surviving server restarts so participant counts and DropCall signals
  // remain correct across deploys/crashes.

  public static addActiveParticipantToGroup(
    groupId: numberOrString, userId: numberOrString,
    callback?: (err: Error, count: number) => void
  ) {
    const key = Keys.forActiveParticipantsOfGroup(groupId);
    const multi = client.multi();
    multi.sadd(key, userId + "");
    multi.expire(key, ACTIVE_STATE_TTL);
    multi.exec(function(err) {
      if (err) { if (callback) { return callback(err, 0); } return; }
      client.scard(key, function(err2, count) {
        if (callback) { return callback(err2, count || 0); }
      });
    });
  }

  public static removeActiveParticipantFromGroup(
    groupId: numberOrString, userId: numberOrString,
    callback?: (err: Error, count: number) => void
  ) {
    const key = Keys.forActiveParticipantsOfGroup(groupId);
    client.srem(key, userId + "", function(err) {
      if (err) { if (callback) { return callback(err, 0); } return; }
      client.scard(key, function(err2, count) {
        if (callback) { return callback(err2, count || 0); }
      });
    });
  }

  public static getActiveParticipantsOfGroup(
    groupId: numberOrString,
    callback: (err: Error, userIds: string[]) => void
  ) {
    client.smembers(Keys.forActiveParticipantsOfGroup(groupId), function(err, userIds) {
      if (err) { return callback(err, []); }
      return callback(null, userIds || []);
    });
  }

  public static clearActiveParticipantsOfGroup(
    groupId: numberOrString,
    callback?: (err: Error) => void
  ) {
    client.del(Keys.forActiveParticipantsOfGroup(groupId), function(err) {
      if (callback) { return callback(err); }
    });
  }

  public static addActiveGroupForUser(
    userId: numberOrString, groupId: numberOrString,
    callback?: (err: Error) => void
  ) {
    const key = Keys.forActiveGroupsOfUser(userId);
    const multi = client.multi();
    multi.sadd(key, groupId + "");
    multi.expire(key, ACTIVE_STATE_TTL);
    multi.exec(function(err) {
      if (callback) { return callback(err); }
    });
  }

  public static removeActiveGroupForUser(
    userId: numberOrString, groupId: numberOrString,
    callback?: (err: Error) => void
  ) {
    client.srem(Keys.forActiveGroupsOfUser(userId), groupId + "", function(err) {
      if (callback) { return callback(err); }
    });
  }

  public static getActiveGroupsOfUser(
    userId: numberOrString,
    callback: (err: Error, groupIds: string[]) => void
  ) {
    client.smembers(Keys.forActiveGroupsOfUser(userId), function(err, groupIds) {
      if (err) { return callback(err, []); }
      return callback(null, groupIds || []);
    });
  }

  public static clearActiveGroupsOfUser(
    userId: numberOrString,
    callback?: (err: Error) => void
  ) {
    client.del(Keys.forActiveGroupsOfUser(userId), function(err) {
      if (callback) { return callback(err); }
    });
  }

  public static setActiveCall(
    userId: numberOrString,
    channelType: number,
    targetId: numberOrString,
    isSos: boolean,
    callback?: (err: Error) => void
  ) {
    const key = Keys.forActiveCallOfUser(userId);
    const multi = client.multi();
    multi.hmset(
      key,
      "channelType", channelType.toString(),
      "targetId", targetId.toString(),
      "isSos", isSos.toString()
    );
    multi.expire(key, ACTIVE_STATE_TTL);
    multi.exec(function(err) {
      if (callback) { return callback(err); }
    });
  }

  public static getActiveCall(
    userId: numberOrString,
    callback: (err: Error, details: { channelType: number; targetId: string; isSos: boolean } | null) => void
  ) {
    client.hgetall(Keys.forActiveCallOfUser(userId), function(err, obj) {
      if (err) { return callback(err, null); }
      if (!obj || Object.keys(obj).length === 0) { return callback(null, null); }
      return callback(null, {
        channelType: parseInt(obj.channelType, 10),
        isSos: obj.isSos === "true",
        targetId: obj.targetId
      });
    });
  }

  public static clearActiveCall(userId: numberOrString, callback?: (err: Error) => void) {
    client.del(Keys.forActiveCallOfUser(userId), function(err) {
      if (callback) { return callback(err); }
    });
  }

  public static refreshActiveCall(userId: numberOrString, callback?: (err: Error) => void) {
    client.expire(Keys.forActiveCallOfUser(userId), ACTIVE_STATE_TTL, function(err) {
      if (callback) { return callback(err); }
    });
  }

  // ---------------------------------------------------------------------------
  // Atomic floor-lock methods (cluster-safe via Redis SET NX EX + Lua script)
  // ---------------------------------------------------------------------------

  /**
   * Try to acquire the private-channel floor for a one-to-one call.
   * Uses SET NX EX so only ONE worker across the entire cluster can win.
   * @param sortedPairKey  Already-sorted "userA|userB" string (from privateFloorKey()).
   * @param userId         The requesting user.
   * @param ttlSeconds     Auto-expire the lock after this many seconds (crash safety).
   * @param callback       acquired=true  → caller owns the floor.
   *                       acquired=false → currentOwner holds it.
   */
  public static acquirePrivateFloor(
    sortedPairKey: string,
    userId: string,
    ttlSeconds: number,
    callback: (err: Error, acquired: boolean, currentOwner: string) => void
  ): void {
    const key = Keys.forPrivateFloor(sortedPairKey);
    // SET key userId NX EX ttl  → "OK" on success, null if key already exists
    (client as any).set(key, userId, "NX", "EX", ttlSeconds, function(err: Error, reply: string) {
      if (err) { return callback(err, false, null); }
      if (reply === "OK") {
        // We won the race
        return callback(null, true, userId);
      }
      // Someone else holds it — find out who
      client.get(key, function(err2: Error, currentOwner: string) {
        if (err2) { return callback(err2, false, null); }
        return callback(null, false, currentOwner || "");
      });
    });
  }

  /**
   * Read-only check: return the current owner of a private-channel floor without
   * modifying any state.  Use this instead of acquirePrivateFloor when you only
   * need to know who holds the floor (e.g. audio-packet ownership validation).
   */
  public static getPrivateFloorOwner(
    sortedPairKey: string,
    callback: (err: Error, owner: string | null) => void
  ): void {
    const key = Keys.forPrivateFloor(sortedPairKey);
    client.get(key, function(err: Error, owner: string) {
      if (err) { return callback(err, null); }
      return callback(null, owner);
    });
  }

  /**
   * Release the private-channel floor.  Only the owner can release it.
   * Uses a Lua script so GET + DEL execute atomically on the Redis server —
   * a different worker cannot sneak in between the check and the delete.
   */
  public static releasePrivateFloor(
    sortedPairKey: string,
    userId: string,
    callback?: (err: Error, released: boolean) => void
  ): void {
    const key = Keys.forPrivateFloor(sortedPairKey);
    const lua = `
      if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
      else
        return 0
      end
    `;
    (client as any).eval(lua, 1, key, userId, function(err: Error, result: number) {
      if (err) { if (callback) { return callback(err, false); } return; }
      if (callback) { return callback(null, result === 1); }
    });
  }

  /**
   * Try to acquire the group floor atomically across all cluster workers.
   * @param groupId     The group whose floor is being contested.
   * @param userId      The requesting user.
   * @param ttlSeconds  Auto-expire TTL (crash safety).
   * @param callback    acquired=true → caller owns the floor.
   */
  public static acquireGroupFloor(
    groupId: numberOrString,
    userId: string,
    ttlSeconds: number,
    callback: (err: Error, acquired: boolean, currentOwner: string) => void
  ): void {
    const key = Keys.forGroupFloor(groupId);
    (client as any).set(key, userId, "NX", "EX", ttlSeconds, function(err: Error, reply: string) {
      if (err) { return callback(err, false, null); }
      if (reply === "OK") {
        return callback(null, true, userId);
      }
      client.get(key, function(err2: Error, currentOwner: string) {
        if (err2) { return callback(err2, false, null); }
        return callback(null, false, currentOwner || "");
      });
    });
  }

  /**
   * Release the group floor — owner-only, atomic Lua script.
   */
  public static releaseGroupFloor(
    groupId: numberOrString,
    userId: string,
    callback?: (err: Error, released: boolean) => void
  ): void {
    const key = Keys.forGroupFloor(groupId);
    const lua = `
      if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
      else
        return 0
      end
    `;
    (client as any).eval(lua, 1, key, userId, function(err: Error, result: number) {
      if (err) { if (callback) { return callback(err, false); } return; }
      if (callback) { return callback(null, result === 1); }
    });
  }

  /**
   * Refresh the TTL on an already-held group floor (called on each audio chunk).
   * No-op when the key doesn't exist (floor was released).
   */
  public static refreshGroupFloor(
    groupId: numberOrString,
    userId: string,
    ttlSeconds: number,
    callback?: (err: Error) => void
  ): void {
    const key = Keys.forGroupFloor(groupId);
    // Only extend TTL when we're still the owner
    const lua = `
      if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("EXPIRE", KEYS[1], ARGV[2])
      else
        return 0
      end
    `;
    (client as any).eval(lua, 1, key, userId, ttlSeconds + "", function(err: Error) {
      if (callback) { return callback(err); }
    });
  }

  // ---------------------------------------------------------------------------

  /**
   * Deletes all transient runtime keys (floor locks, active-call state) from Redis.
   * Called once on server startup so stale entries from a previous process/container
   * don't corrupt in-memory state that has been reset to zero.
   * Permanent data (user info, group membership, messages) is NOT touched.
   */
  public static clearRuntimeState(callback?: (err: Error) => void): void {
    // Patterns covering all volatile call-session keys:
    //   pf.*   → private floor locks
    //   gf.*   → group floor locks
    //   u.*.ac → per-user active-call hash
    //   u.*.ag → per-user active-groups set
    //   g.*.ap → per-group active-participants set
    //   op.*   → operation dedup keys (START/STOP idempotency, 15s TTL).
    //            These MUST be cleared on restart: the app retries the last
    //            in-flight START with the same operationId after reconnecting,
    //            and withOperationDedupe silently drops it if the op.* key
    //            still exists — so the post-restart call never reaches the receiver.
    const patterns = ["pf.*", "gf.*", "u.*.ac", "u.*.ag", "g.*.ap", "op.*"];
    let pending = patterns.length;
    let firstErr: Error = null;

    function scanAndDelete(pattern: string, cursor: string, done: (err: Error) => void) {
      (client as any).scan(cursor, "MATCH", pattern, "COUNT", "200", function(err: Error, reply: any) {
        if (err) { return done(err); }
        const nextCursor: string = reply[0];
        const keys: string[] = reply[1];
        if (keys && keys.length > 0) {
          client.del.apply(client, [...keys, function(delErr: Error) {
            if (delErr) { return done(delErr); }
            if (nextCursor === "0") { return done(null); }
            scanAndDelete(pattern, nextCursor, done);
          }]);
        } else {
          if (nextCursor === "0") { return done(null); }
          scanAndDelete(pattern, nextCursor, done);
        }
      });
    }

    patterns.forEach(function(pattern) {
      scanAndDelete(pattern, "0", function(err) {
        if (err && !firstErr) { firstErr = err; }
        pending--;
        if (pending === 0) {
          if (firstErr) {
            logger.error("Redis.clearRuntimeState error:", firstErr);
          } else {
            logger.info("Redis.clearRuntimeState: stale call state cleared");
          }
          if (callback) { callback(firstErr); }
        }
      });
    });
  }

  public static periodicClean() {
    if (cleanInterval) { return; }
    cleanInterval = setInterval(function() {
      const group = cleanGroup;
      const key = Keys.forMessagesOfGroup(group);
      if (CLEAN_LOG_ENABLED) {
        client.lrange(key, 0, 200, function(err1, messageIds1) {
          client.lrange(key, 0, 49, function(err2, messageIds2) {
            if (err1 || err2) {
              logger.info(`periodicClean before group ${group} err ${err1} err2 ${err2}`);
            } else {
              logger.info(`periodicClean before group ${group} 0-200 ${messageIds1} 0-49 ${messageIds2}`);
            }
          });
        });
      }
      if (!DRY_CLEAN_ENABLED) {
        client.ltrim(key, 0, 49, function(err, reply) {
          if (err) {
            logger.info(`periodicClean ltrim group ${group} err ${err}`);
          } else {
            logger.info(`periodicClean ltrim group ${group} ${reply}`);
          }
          if (CLEAN_LOG_ENABLED) {
            client.lrange(key, 0, 200, function(err1, messageIds1) {
              client.lrange(key, 0, 49, function(err2, messageIds2) {
                if (err1 || err2) {
                  logger.info(`periodicClean after group ${group} err ${err1} err2 ${err2}`);
                } else {
                  logger.info(`periodicClean after group ${group} 0-200 ${messageIds1} 0-49 ${messageIds2}`);
                }
              });
            });
          }
        });
      }
      if (cleanGroup > CLEAN_GROUPS_AMOUNT) {
        cleanGroup = 1;
      } else {
        cleanGroup++;
      }
    }, CLEAN_INTERVAL);
  }
}

export = Redis;
