import * as cluster from "cluster";
import * as dbug from "debug";
import * as jwt from "jwt-simple";

import config = require("./config");
import { Keys } from "./keys";
import Redis = require("./redis");
import { IMessage, numberOrString } from "./types";

const dbug1 = dbug("vp:states");
function debug(msg: string) {
  dbug1((cluster.worker ? `worker ${cluster.worker.id} ` : "") + msg);
}

const SECRET = config.secretKey;
const GROUPS_BUSY_TIMEOUT = config.group.busyTimeout;
const GROUPS_INSPECT_INTERVAL = config.group.inspectInterval;

let inspectInterval: NodeJS.Timer;
let memored: any;

const usersInsideGroupsSet = {};
const usersCurrentMessagesSet = {};
const groupsOfUsersSet = {};
const groupsCurrentMessagesSet = {};
const groupsActiveParticipantsSet = {};
const activeCallGroupsOfUsersSet = {};
const groupsDroppedParticipantsSet = {};

// Local index of private-floor keys owned by each user — used ONLY for the
// disconnect sweep.  The authoritative lock lives in Redis (SET NX EX).
const privateFloorKeysByUser: { [userId: string]: Set<string> } = {};

// Floor TTL: max call duration + a generous buffer (seconds).
const PRIVATE_FLOOR_TTL_SEC = Math.ceil((config.group.busyTimeout / 1000) + 30);
const GROUP_FLOOR_TTL_SEC   = Math.ceil((config.group.busyTimeout / 1000) + 30);

export function privateFloorKey(userId1: numberOrString, userId2: numberOrString): string {
  const u1 = userId1 + "";
  const u2 = userId2 + "";
  return u1 < u2 ? `${u1}|${u2}` : `${u2}|${u1}`;
}

interface IMessage2 {
  audioTime?: number;
  channelType?: number;
  fromId: numberOrString;
  messageType?: number;
  startTime: number;
  toId?: numberOrString;
}

export default class States {

  public static setMemored(memo) {
    memored = memo;
  }

  /**
   * Decode user token from JWT format into user uuid
   *
   * @param { string } token
   * @param { function } callback
   * @private
   *
   */
  public static getUserFromToken(token: string, callback: (err: Error, user: any) => void) {
    try {
      const user = jwt.decode(token, SECRET);
      return callback(null, user);
    } catch (err) {
      return callback(err, null);
    }
  }

  public static addUserToGroup(
    userId: numberOrString,
    groupId: numberOrString,
    callback?: (err: Error, succeed: boolean) => void
  ) {
    userId = userId + "";
    groupId = groupId + "";
    debug(`*** STORE in addUserToGroup groupId:${groupId} userIds:${userId} ***`);
    States.getUsersInsideGroup(groupId, function(err, userIds) {
      if (userIds && userIds instanceof Array) {
        if (!userIds.includes(userId)) { userIds.push(userId); }
      } else {
        userIds = [userId];
      }
      States.setUsersInsideGroup(groupId, userIds);
      States.getGroupsOfUser(userId, function(error, groupIds) {
        if (groupIds && groupIds instanceof Array) {
          if (!groupIds.includes(groupId)) { groupIds.push(groupId); }
        } else {
          groupIds = [groupId];
        }
        States.setGroupsOfUser(userId, groupIds);
        if (callback) { return callback(null, true); }
      });
    });
  }

  public static setUsersInsideGroup(
    groupId: number|string, userIds: Array<number|string>,
    callback?: (err, succeed) => void) {
    groupId = groupId + "";
    userIds = userIds.map((userId) => userId + "");
    usersInsideGroupsSet[groupId] = userIds;
    if (!!memored) {
      memored.store(Keys.forUsersInsideGroup(groupId), userIds, function() {
        debug(`STORE in setUsersInsideGroup groupId:${groupId} userIds:${JSON.stringify(userIds)}`);
      });
    }
    if (!!callback) { return callback(null, true); }
    return;
  }

  public static getUsersInsideGroup(
    groupId: number|string,
    callback: (
      err: Error,
      userIds: Array<number|string>
    ) => void
  ) {
    groupId = groupId + "";
    if (!memored) {
      return callback(null, usersInsideGroupsSet[groupId]);
    } else {
      memored.read(Keys.forUsersInsideGroup(groupId), function(err, userIds) {
        usersInsideGroupsSet[groupId] = userIds;
        debug(`STORE in getUsersInsideGroup groupId:${groupId} userIds:${JSON.stringify(userIds)}`);
        return callback(null, userIds);
      });
    }
  }

  public static getActiveParticipantsOfGroup(
    groupId: numberOrString,
    callback: (err: Error, userIds: Array<number|string>) => void
  ) {
    groupId = groupId + "";
    const inMemory = (groupsActiveParticipantsSet[groupId] || []);
    if (inMemory.length > 0) {
      return callback(null, inMemory.slice());
    }
    // Fallback to Redis (e.g. after a server restart)
    Redis.getActiveParticipantsOfGroup(groupId, (err, userIds) => {
      if (!err && userIds && userIds.length > 0) {
        groupsActiveParticipantsSet[groupId] = userIds;
      }
      return callback(err, userIds || []);
    });
  }

  public static addActiveParticipantToGroup(
    groupId: numberOrString,
    userId: numberOrString,
    callback?: (err: Error, count: number) => void
  ) {
    groupId = groupId + "";
    userId = userId + "";
    const current = (groupsActiveParticipantsSet[groupId] || []).map((id) => id + "");
    if (!current.includes(userId)) {
      current.push(userId);
    }
    groupsActiveParticipantsSet[groupId] = current;
    debug(
      `ACTIVE_PARTICIPANTS add groupId:${groupId} userId:${userId}` +
      ` count:${current.length} users:${JSON.stringify(current)}`
    );
    Redis.addActiveParticipantToGroup(groupId, userId); // persist with TTL for restart recovery
    if (callback) { return callback(null, current.length); }
    return;
  }

  public static addUserToActiveCallGroup(
    userId: numberOrString,
    groupId: numberOrString,
    callback?: (err: Error, count: number) => void
  ) {
    userId = userId + "";
    groupId = groupId + "";
    const droppedUsers = (groupsDroppedParticipantsSet[groupId] || []).map((id) => id + "");
    if (droppedUsers.includes(userId)) {
      groupsDroppedParticipantsSet[groupId] = droppedUsers.filter((id) => id !== userId);
    }
    const currentGroups = (activeCallGroupsOfUsersSet[userId] || []).map((id) => id + "");
    if (!currentGroups.includes(groupId)) {
      currentGroups.push(groupId);
    }
    activeCallGroupsOfUsersSet[userId] = currentGroups;
    Redis.addActiveGroupForUser(userId, groupId); // persist for restart recovery
    return States.addActiveParticipantToGroup(groupId, userId, callback);
  }

  public static removeUserFromActiveCallGroup(
    userId: numberOrString,
    groupId: numberOrString,
    callback?: (err: Error, count: number) => void
  ) {
    userId = userId + "";
    groupId = groupId + "";
    const droppedUsers = (groupsDroppedParticipantsSet[groupId] || []).map((id) => id + "");
    if (!droppedUsers.includes(userId)) {
      droppedUsers.push(userId);
    }
    groupsDroppedParticipantsSet[groupId] = droppedUsers;
    const currentGroups = (activeCallGroupsOfUsersSet[userId] || [])
      .map((id) => id + "")
      .filter((id) => id !== groupId);
    activeCallGroupsOfUsersSet[userId] = currentGroups;
    Redis.removeActiveGroupForUser(userId, groupId);
    return States.removeActiveParticipantFromGroup(groupId, userId, callback);
  }

  public static getActiveCallGroupsOfUser(
    userId: numberOrString,
    callback: (err: Error, groupIds: Array<number|string>) => void
  ) {
    userId = userId + "";
    return callback(null, (activeCallGroupsOfUsersSet[userId] || []).slice());
  }

  public static clearActiveCallGroup(
    groupId: numberOrString,
    callback?: (err: Error, count: number) => void
  ) {
    groupId = groupId + "";
    delete groupsDroppedParticipantsSet[groupId];
    Object.keys(activeCallGroupsOfUsersSet).forEach((userId) => {
      activeCallGroupsOfUsersSet[userId] = (activeCallGroupsOfUsersSet[userId] || [])
        .map((id) => id + "")
        .filter((id) => id !== groupId);
      Redis.removeActiveGroupForUser(userId, groupId);
    });
    return States.clearActiveParticipantsOfGroup(groupId, callback);
  }

  public static removeActiveParticipantFromGroup(
    groupId: numberOrString,
    userId: numberOrString,
    callback?: (err: Error, count: number) => void
  ) {
    groupId = groupId + "";
    userId = userId + "";
    const current = (groupsActiveParticipantsSet[groupId] || [])
      .map((id) => id + "")
      .filter((id) => id !== userId);
    groupsActiveParticipantsSet[groupId] = current;
    debug(
      `ACTIVE_PARTICIPANTS remove groupId:${groupId} userId:${userId}` +
      ` count:${current.length} users:${JSON.stringify(current)}`
    );
    Redis.removeActiveParticipantFromGroup(groupId, userId);
    if (callback) { return callback(null, current.length); }
    return;
  }

  public static clearActiveParticipantsOfGroup(
    groupId: numberOrString,
    callback?: (err: Error, count: number) => void
  ) {
    groupId = groupId + "";
    groupsActiveParticipantsSet[groupId] = [];
    delete groupsDroppedParticipantsSet[groupId];
    debug(`ACTIVE_PARTICIPANTS clear groupId:${groupId} count:0 users:[]`);
    Redis.clearActiveParticipantsOfGroup(groupId);
    if (callback) { return callback(null, 0); }
    return;
  }

  public static getActiveParticipantCountOfGroup(
    groupId: numberOrString,
    callback: (err: Error, count: number) => void
  ) {
    groupId = groupId + "";
    return callback(null, (groupsActiveParticipantsSet[groupId] || []).length);
  }

  public static removeActiveParticipantFromAllGroups(
    userId: numberOrString,
    callback?: (err: Error, removedGroups: Array<number|string>) => void
  ) {
    userId = userId + "";
    const inMemoryGroups: Array<number|string> =
      (activeCallGroupsOfUsersSet[userId] || []).map((id) => id + "");

    const doRemove = (groupIds: Array<number|string>) => {
      delete activeCallGroupsOfUsersSet[userId];
      Redis.clearActiveGroupsOfUser(userId);
      groupIds.forEach((groupId) => {
        const current = (groupsActiveParticipantsSet[groupId] || []).map((id) => id + "");
        groupsActiveParticipantsSet[groupId] = current.filter((id) => id !== userId + "");
        debug(
          `ACTIVE_PARTICIPANTS disconnect-remove groupId:${groupId} userId:${userId}` +
          ` count:${groupsActiveParticipantsSet[groupId].length}` +
          ` users:${JSON.stringify(groupsActiveParticipantsSet[groupId])}`
        );
        Redis.removeActiveParticipantFromGroup(groupId, userId);
      });
      if (callback) { return callback(null, groupIds); }
    };

    if (inMemoryGroups.length > 0) {
      return doRemove(inMemoryGroups);
    }

    // Fallback to Redis when in-memory is empty (e.g. after a server restart)
    Redis.getActiveGroupsOfUser(userId, (err, redisGroups) => {
      doRemove(redisGroups || []);
    });
  }

  public static getGroupsWithActiveParticipant(
    userId: numberOrString,
    callback: (err: Error, groupIds: Array<number|string>) => void
  ) {
    userId = userId + "";
    const inMemory = (activeCallGroupsOfUsersSet[userId] || []).map((id) => id + "");
    if (inMemory.length > 0) {
      return callback(null, inMemory);
    }
    // Fallback to Redis (e.g. after a server restart where in-memory was wiped)
    Redis.getActiveGroupsOfUser(userId, (err, groupIds) => {
      if (!err && groupIds && groupIds.length > 0) {
        activeCallGroupsOfUsersSet[userId] = groupIds;
      }
      return callback(err, groupIds || []);
    });
  }

  public static releaseFloorOwnershipForUser(
    userId: numberOrString,
    callback?: (err: Error, releasedGroups: Array<number|string>) => void
  ) {
    userId = userId + "";
    const releasedGroups: Array<number|string> = [];
    Object.keys(groupsCurrentMessagesSet).forEach((groupId) => {
      const message = groupsCurrentMessagesSet[groupId];
      if (message && (message.fromId + "") === userId) {
        groupsCurrentMessagesSet[groupId] = null;
        releasedGroups.push(groupId);
        if (memored) {
          memored.remove(Keys.forCurrentMessageOfGroup(groupId), function() {
            return;
          });
        }
      }
    });
    if (callback) { return callback(null, releasedGroups); }
    return;
  }

  public static setGroupsOfUser(
    userId: number|string, groupIds: Array<number|string>,
    callback?: (err, succeed) => void) {
    userId = userId + "";
    groupIds = groupIds.map((groupId) => groupId + "");
    groupsOfUsersSet[userId] = groupIds;
    if (!!memored) {
      memored.store(Keys.forGroupsOfUser(userId), groupIds, function() {
        debug(`STORE in setGroupsOfUser userId:${userId} userIds:${JSON.stringify(groupIds)}`);
      });
    }
    if (!!callback) { return callback(null, true); }
    return;
  }

  public static getGroupsOfUser(userId: number|string, callback: (err, groupIds) => void) {
    userId = userId + "";
    if (!memored) {
      return callback(null, groupsOfUsersSet[userId]);
    } else {
      memored.read(Keys.forGroupsOfUser(userId), function(err, groupIds) {
        groupsOfUsersSet[userId] = groupIds;
        return callback(null, groupIds);
      });
    }
  }

  public static setCurrentMessageOfUser(
    userId: numberOrString, msg: IMessage,
    callback?: (err: Error, succeed: boolean) => void) {
    userId = userId + "";
    const now = Date.now();
    const message: IMessage2 = {
      audioTime: now,
      channelType: msg.channelType,
      fromId: msg.fromId,
      messageType: msg.messageType,
      startTime: now,
      toId: msg.toId
    };
    if (!memored) {
      usersCurrentMessagesSet[userId] = message;
      if (callback) { return callback(null, true); }
      return;
    } else {
      memored.store(Keys.forCurrentMessageOfUser(userId), message, function() {
        debug(`STORE in setCurrentMessageOfUser userId:${userId} msg:${JSON.stringify(msg)}`);
        usersCurrentMessagesSet[userId] = message;
        if (callback) { return callback(null, true); }
        return;
      });
    }
  }

  public static removeCurrentMessageOfUser(
    userId: numberOrString,
    callback?: (err: Error, succeed: boolean) => void) {
    userId = userId + "";
    if (!memored) {
      usersCurrentMessagesSet[userId] = null;
      if (callback) { return callback(null, true); }
      return;
    } else {
      memored.remove(Keys.forCurrentMessageOfUser(userId), function() {
        usersCurrentMessagesSet[userId] = null;
        if (callback) { return callback(null, true); }
        return;
      });
    }
  }

  public static getCurrentMessageOfGroup(
    groupId: numberOrString,
    callback: (err: Error, msg: IMessage2) => void) {
    groupId = groupId + "";
    if (!memored) {
      const msg: IMessage2 = groupsCurrentMessagesSet[groupId];
      return callback(null, msg);
    } else {
      memored.read(Keys.forCurrentMessageOfGroup(groupId), function(err, message) {
        const msg: IMessage2 = message;
        groupsCurrentMessagesSet[groupId] = msg;
        return callback(null, msg);
      });
    }
  }

  public static setCurrentMessageOfGroup(
    groupId: numberOrString, msg: IMessage,
    callback?: (err: Error, succeed: boolean) => void) {
    groupId = groupId + "";
    const now = Date.now();
    const message: IMessage2 = {
      audioTime: now,
      channelType: msg.channelType,
      fromId: msg.fromId,
      messageType: msg.messageType,
      startTime: now,
      toId: msg.toId
    };
    if (!memored) {
      groupsCurrentMessagesSet[groupId] = message;
      if (callback) { return callback(null, true); }
      return;
    } else {
      memored.store(Keys.forCurrentMessageOfGroup(groupId), message, function() {
        debug(`STORE in setCurrentMessageOfGroup groupId:${groupId} msg:${JSON.stringify(msg)}`);
        groupsCurrentMessagesSet[groupId] = message;
        if (callback) { return callback(null, true); }
        return;
      });
    }
  }

  public static removeCurrentMessageOfGroup(
    groupId: numberOrString,
    callback?: (err: Error, succeed: boolean) => void) {
    groupId = groupId + "";
    if (!memored) {
      groupsCurrentMessagesSet[groupId] = null;
      if (callback) { return callback(null, true); }
      return;
    } else {
      memored.remove(Keys.forCurrentMessageOfGroup(groupId), function() {
        groupsCurrentMessagesSet[groupId] = null;
        if (callback) { return callback(null, true); }
        return;
      });
    }
  }

  public static getBusyStateOfGroup(
    groupId: numberOrString,
    callback: (err: Error, busyWithUserId: numberOrString) => void) {
    groupId = groupId + "";
    if (!memored) {
      const message: IMessage2 = groupsCurrentMessagesSet[groupId];
      debug(`getBusyStateOfGroup => groupId: ${groupId} no memored, message: ${JSON.stringify(message)}`);
      if (!!message && message.fromId) {
        callback(null, message.fromId);
      } else {
        callback(null, 0);
      }
    } else {
      memored.read(Keys.forCurrentMessageOfGroup(groupId), function(err, message) {
        debug(`getBusyStateOfGroup => groupId: ${groupId} with memored, message: ${JSON.stringify(message)}`);
        groupsCurrentMessagesSet[groupId] = message;
        if (!!message && message.fromId) {
          callback(null, message.fromId);
        } else {
          callback(null, 0);
        }
      });
    }
  }

  public static isFloorOwnerOfGroup(
    groupId: numberOrString,
    userId: numberOrString,
    callback: (err: Error, isOwner: boolean) => void
  ) {
    groupId = groupId + "";
    userId = userId + "";
    States.getBusyStateOfGroup(groupId, (err, busyWithUserId) => {
      if (err) { return callback(err, false); }
      return callback(null, !!busyWithUserId && busyWithUserId + "" === userId);
    });
  }

  public static acquireFloorOfGroup(
    groupId: numberOrString,
    userId: numberOrString,
    callback: (err: Error, acquired: boolean, floorOwner: numberOrString) => void
  ) {
    groupId = groupId + "";
    userId = userId + "";
    // Use atomic Redis SET NX EX — only ONE worker across the cluster can win.
    Redis.acquireGroupFloor(groupId, userId, GROUP_FLOOR_TTL_SEC, (err, acquired, currentOwner) => {
      if (err) { return callback(err, false, 0); }
      if (acquired) {
        // Mirror into local message state so the rest of the code (audioTime
        // inspection, floor-owner checks) keeps working as before.
        States.setBusyStateOfGroup(groupId, userId, (setErr) => {
          if (setErr) {
            // Roll back the Redis lock so we don't leave a phantom floor.
            Redis.releaseGroupFloor(groupId, userId);
            return callback(setErr, false, userId);
          }
          return callback(null, true, userId);
        });
      } else {
        return callback(null, false, currentOwner || "0");
      }
    });
  }

  public static refreshFloorOfGroup(
    groupId: numberOrString,
    userId: numberOrString,
    callback?: (err: Error, refreshed: boolean) => void
  ) {
    groupId = groupId + "";
    userId = userId + "";
    States.isFloorOwnerOfGroup(groupId, userId, (err, isOwner) => {
      if (err || !isOwner) {
        if (callback) { return callback(err, false); }
        return;
      }
      States.updateAudioTimeOfGroup(groupId, callback);
    });
  }

  public static releaseFloorOfGroup(
    groupId: numberOrString,
    userId: numberOrString,
    callback?: (err: Error, released: boolean) => void
  ) {
    groupId = groupId + "";
    userId = userId + "";
    // Atomically release the Redis lock (owner-only Lua script), then clear
    // the local mirror regardless so stale in-memory state doesn't linger.
    Redis.releaseGroupFloor(groupId, userId, (redisErr, released) => {
      if (redisErr) {
        debug(`releaseFloorOfGroup Redis error groupId:${groupId} userId:${userId} err:${redisErr}`);
      }
      // Clear local mirror unconditionally — even if redis said "not owner"
      // (e.g. TTL expired) we still want to clean up local state.
      States.removeBusyStateOfGroup(groupId, function(removeErr) {
        if (callback) { return callback(removeErr || null, released || false); }
      });
    });
  }

  public static setBusyStateOfGroup(
    groupId: numberOrString, busyWithUserId: numberOrString,
    callback?: (err: Error, busyWithUserId: numberOrString) => void) {
    groupId = groupId + "";
    busyWithUserId = busyWithUserId + "";
    if (!busyWithUserId || busyWithUserId === "" || busyWithUserId === "0") {
      delete groupsCurrentMessagesSet[groupId];
      if (!memored) {
        if (callback) { return callback(null, busyWithUserId); }
        return;
      }
      memored.remove(Keys.forCurrentMessageOfGroup(groupId), function() {
        if (callback) { return callback(null, busyWithUserId); }
        return;
      });
    } else {
      const now = Date.now();
      const busyMessage: IMessage2 = {
        audioTime: now,
        fromId: busyWithUserId,
        startTime: now
      };
      if (!memored) {
        const message = groupsCurrentMessagesSet[groupId];
        if (!message || message.fromId !== busyWithUserId) {
          groupsCurrentMessagesSet[groupId] = busyMessage;
        }
        if (callback) { return callback(null, busyWithUserId); }
        return;
      } else {
        memored.read(Keys.forCurrentMessageOfGroup(groupId), function(err, message) {
          if (!message || message.fromId !== busyWithUserId) {
            memored.store(Keys.forCurrentMessageOfGroup(groupId), busyMessage, function() {
              debug(`STORE in setBusyStateOfGroup groupId:${groupId} busyWithUserId:${busyWithUserId}`);
              groupsCurrentMessagesSet[groupId] = busyMessage;
              if (callback) { return callback(null, busyWithUserId); }
              return;
            });
          } else {
            if (callback) { return callback(null, message.fromId); }
            return;
          }
        });
      }
    }
  }

  public static removeBusyStateOfGroup(groupId: numberOrString,
                                       callback?: (err: Error, busyWithUserId: numberOrString) => void) {
    groupId = groupId + "";
    States.setBusyStateOfGroup(groupId, 0, callback);
  }

  public static updateAudioTimeOfGroup(groupId: numberOrString, callback?: (err: Error, succeed: boolean) => void) {
    groupId = groupId + "";
    States.getCurrentMessageOfGroup(groupId, function(err, message) {
      if (message) {
        message.audioTime = Date.now();
      } else {
        if (callback) { return callback(null, false); }
        return;
      }

      if (!memored) {
        groupsCurrentMessagesSet[groupId] = message;
        if (callback) { return callback(null, true); }
        return;
      } else {
        memored.store(Keys.forCurrentMessageOfGroup(groupId), message, function() {
          debug(`STORE in updateAudioTimeOfGroup groupId:${groupId} message:${JSON.stringify(message)}`);
          groupsCurrentMessagesSet[groupId] = message;
          if (callback) { return callback(null, true); }
          return;
        });
      }

    });
  }

  public static getAudioTimeOfGroup(groupId: numberOrString, callback: (err: Error, audioTime: number) => void) {
    groupId = groupId + "";
    States.getCurrentMessageOfGroup(groupId, function(err, message) {
      let audioTime = null;
      if (message && message.audioTime) { audioTime = message.audioTime; }
      callback(null, audioTime);
    });
  }

  public static removeKeyUsersInsideGroup(
    groupId: number|string,
    callback?: (err, succeed) => void) {
      groupId = groupId + "";
      if (!memored) {
        usersInsideGroupsSet[groupId] = undefined;
        return callback(null, true);
      } else {
        memored.remove(Keys.forUsersInsideGroup(groupId), () => {
          usersInsideGroupsSet[groupId] = undefined;
          debug(`STORE in removeKeyUsersInsideGroup groupId:${groupId}`);
          return callback(null, true);
        });
      }
  }

  // ---------------------------------------------------------------------------
  // Private-channel floor management  (cluster-safe — backed by Redis)
  // ---------------------------------------------------------------------------

  /**
   * Atomically acquire the floor for a one-to-one private conversation.
   * Uses Redis SET NX EX so only ONE worker in the cluster can win the race.
   * The floor key is symmetric: acquirePrivateFloor(A,B) and acquirePrivateFloor(B,A)
   * compete for the same Redis key, so only one side can talk at a time.
   */
  public static acquirePrivateFloor(
    userId1: numberOrString,
    userId2: numberOrString,
    requestingUserId: numberOrString,
    callback: (err: Error, acquired: boolean, currentOwner: numberOrString) => void
  ): void {
    const key = privateFloorKey(userId1, userId2);
    const requester = requestingUserId + "";
    Redis.acquirePrivateFloor(key, requester, PRIVATE_FLOOR_TTL_SEC, (err, acquired, currentOwner) => {
      if (err) { return callback(err, false, 0); }
      if (acquired) {
        // Track locally so we can release on disconnect.
        if (!privateFloorKeysByUser[requester]) { privateFloorKeysByUser[requester] = new Set(); }
        privateFloorKeysByUser[requester].add(key);
        debug(`acquirePrivateFloor key:${key} owner:${requester}`);
      }
      return callback(null, acquired, currentOwner || requester);
    });
  }

  /**
   * Release the private-channel floor.  Only the current owner can release it.
   * Uses a Lua script (GET + DEL) so the check-and-delete is atomic in Redis.
   */
  public static releasePrivateFloor(
    userId1: numberOrString,
    userId2: numberOrString,
    requestingUserId: numberOrString,
    callback?: (err: Error, released: boolean) => void
  ): void {
    const key = privateFloorKey(userId1, userId2);
    const requester = requestingUserId + "";
    Redis.releasePrivateFloor(key, requester, (err, released) => {
      if (!err && released) {
        // Remove from local index.
        if (privateFloorKeysByUser[requester]) { privateFloorKeysByUser[requester].delete(key); }
        debug(`releasePrivateFloor key:${key}`);
      }
      if (callback) { return callback(err || null, released || false); }
    });
  }

  /**
   * Check whether a given user owns the private-channel floor.
   * Reads from Redis so the answer is accurate across all cluster workers.
   */
  public static isPrivateFloorOwner(
    userId1: numberOrString,
    userId2: numberOrString,
    userId: numberOrString,
    callback: (err: Error, isOwner: boolean) => void
  ): void {
    const key = privateFloorKey(userId1, userId2);
    const userIdStr = userId + "";
    Redis.acquirePrivateFloor(key, userIdStr, PRIVATE_FLOOR_TTL_SEC, (err, acquired, currentOwner) => {
      if (err) { return callback(err, false); }
      // We only wanted to check, not grab — if we accidentally acquired, release immediately.
      if (acquired) {
        Redis.releasePrivateFloor(key, userIdStr);
        return callback(null, false); // floor was free, so we're not an existing owner
      }
      return callback(null, currentOwner === userIdStr);
    });
  }

  /**
   * Release every private-channel floor slot held by the given user.
   * Called on disconnect so a dropped connection never leaves the floor permanently locked.
   */
  public static releasePrivateFloorOwnershipForUser(userId: numberOrString): void {
    const userIdStr = userId + "";
    const keys = privateFloorKeysByUser[userIdStr];
    if (!keys || keys.size === 0) { return; }
    keys.forEach((key) => {
      Redis.releasePrivateFloor(key, userIdStr, () => {
        debug(`releasePrivateFloor on disconnect userId:${userIdStr} key:${key}`);
      });
    });
    delete privateFloorKeysByUser[userIdStr];
  }

  // ---------------------------------------------------------------------------

  public static periodicInspect() {
    if (inspectInterval) { return; }

    inspectInterval = setInterval(function() {
      const groupIds = Object.keys(groupsCurrentMessagesSet);
      debug(`inspectInterval: ${groupIds.length}: ${JSON.stringify(groupsCurrentMessagesSet)}`);

      // Process groups one-at-a-time via setImmediate to avoid blocking the event loop
      // when there are many active groups (prevents ping/pong and message handling delays).
      let index = 0;
      function processNext() {
        if (index >= groupIds.length) { return; }
        const groupId = groupIds[index++];
        const message: IMessage2 = groupsCurrentMessagesSet[groupId];
        if (!!message) {
          const userId = message.fromId;
          const startTime = message.startTime;
          if (!!userId && !!startTime) {
            const duration = Date.now() - startTime;
            if (duration > GROUPS_BUSY_TIMEOUT) {
              States.removeBusyStateOfGroup(groupId);
              debug(`GROUPS_BUSY_TIMEOUT userId: ${userId} takes ${duration}` +
                    ` more than ${GROUPS_BUSY_TIMEOUT} talking,` +
                    ` channel is no longer busy`);
            }
          }
        }
        setImmediate(processNext);
      }
      processNext();
    }, GROUPS_INSPECT_INTERVAL);
  }
}

/*
interface IOptions {
  groupsBusyTimeout: number;
  groupsInspectInterval: number;
  memored: any;
  secret: string;
}
*/
