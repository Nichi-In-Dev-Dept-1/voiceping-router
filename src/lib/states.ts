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
const userGroupCallState: { [userId: string]: { groupId: string; isSos: boolean } } = {};
// In-memory backing for private call state — mirrors the Redis u.X.ac key so that
// overlap detection works even when Redis is unavailable or has not yet persisted.
const userPrivateCallState: { [userId: string]: { peerId: string; isSos: boolean } } = {};

// Local index of private-floor keys owned by each user — used ONLY for the
// disconnect sweep.  The authoritative lock lives in Redis (SET NX EX).
const privateFloorKeysByUser: { [userId: string]: Set<string> } = {};

const groupSosState: { [groupId: string]: boolean } = {};

// Tracks which group members actually received a START for the current floor session.
// undefined  → no restriction (normal group call, broadcast to all)
// []         → all members were busy; drop AUDIO until floor released
// [id, ...]  → subset delivery; AUDIO goes only to these members
interface IGroupFloorRecipientSession {
  ownerId: string;
  recipients: string[];
  sessionEpoch: number;
}
const groupFloorRecipients: { [groupId: string]: IGroupFloorRecipientSession } = {};
const groupFloorEpochById: { [groupId: string]: number } = {};
const groupFloorKeysByUser: { [userId: string]: Set<string> } = {};

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

  // Synchronously seeds in-memory group membership from data already known at
  // login (JWT channelIds). Prevents the race where a PTT START arrives before
  // the async Redis.addUserToGroup callback has populated the in-memory state.
  public static addUserToGroupImmediate(userId: numberOrString, groupId: numberOrString) {
    const uid = userId + "";
    const gid = groupId + "";
    const userIds: string[] = ((usersInsideGroupsSet[gid] || []) as any[]).map((u) => u + "");
    if (!userIds.includes(uid)) { userIds.push(uid); }
    usersInsideGroupsSet[gid] = userIds;

    const groupIds: string[] = ((groupsOfUsersSet[uid] || []) as any[]).map((g) => g + "");
    if (!groupIds.includes(gid)) { groupIds.push(gid); }
    groupsOfUsersSet[uid] = groupIds;
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
      const inMemory = usersInsideGroupsSet[groupId];
      if (inMemory && (inMemory as any[]).length > 0) {
        return callback(null, inMemory);
      }
      // In-memory empty — fall back to Redis (handles reconnect race where
      // registerClient's async addUserToGroup hasn't completed yet).
      return Redis.getUsersInsideGroup(groupId, (err, userIds) => {
        if (!err && userIds && userIds.length > 0) {
          usersInsideGroupsSet[groupId] = userIds;
        }
        return callback(null, userIds);
      });
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
    isSos: boolean = false,
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
    userGroupCallState[userId + ""] = { groupId: groupId + "", isSos };
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
    if (userGroupCallState[userId + ""] && userGroupCallState[userId + ""].groupId === groupId + "") {
      delete userGroupCallState[userId + ""];
    }
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
    delete groupSosState[groupId];
    // Update in-memory state synchronously so any in-flight overlap checks see
    // consistent data immediately, before Redis persistence completes.
    Object.keys(activeCallGroupsOfUsersSet).forEach((userId) => {
      activeCallGroupsOfUsersSet[userId] = (activeCallGroupsOfUsersSet[userId] || [])
        .map((id) => id + "")
        .filter((id) => id !== groupId);
      if (userGroupCallState[userId] && userGroupCallState[userId].groupId === groupId + "") {
        delete userGroupCallState[userId];
      }
    });
    // Fire Redis cleanup after in-memory is consistent (fire-and-forget per user is fine;
    // clearActiveParticipantsOfGroup callback is the authoritative completion signal).
    Object.keys(activeCallGroupsOfUsersSet).forEach((userId) => {
      Redis.removeActiveGroupForUser(userId, groupId);
      if (activeCallGroupsOfUsersSet[userId].length === 0) {
        Redis.clearActiveCall(userId);
      }
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
      Redis.clearActiveCall(userId);
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

  // ── Per-user active private call tracking (overlap-call detection) ──────────

  public static setUserPrivateCall(userId: numberOrString, peerId: numberOrString, isSos: boolean) {
    userPrivateCallState[userId + ""] = { peerId: peerId + "", isSos };
    Redis.setActiveCall(userId, 1, peerId, isSos);
  }

  public static setUserGroupCallState(userId: numberOrString, groupId: numberOrString, isSos: boolean) {
    Redis.setActiveCall(userId, 2, groupId, isSos);
  }

  public static clearUserActiveCall(userId: numberOrString) {
    delete userPrivateCallState[userId + ""];
    Redis.clearActiveCall(userId);
  }

  public static refreshUserActiveCall(userId: numberOrString) {
    Redis.refreshActiveCall(userId);
  }

  public static clearUserPrivateCall(userId: numberOrString) {
    delete userPrivateCallState[userId + ""];
    Redis.clearActiveCall(userId);
  }

  /**
   * Returns the peerId the user is currently in a private call with, or null if not in one.
   * Reads in-memory only — synchronous, no async. Used to capture peer before clearing state.
   */
  public static getPrivateCallPeer(userId: numberOrString): string | null {
    const state = userPrivateCallState[userId + ""];
    return state ? state.peerId : null;
  }

  /**
   * Returns the active call state for a user:
   * - inCall: true if the user is in any active private or group call
   * - isSos:  true if that call is an SOS call
   */
  public static isUserInAnyActiveCall(
    userId: numberOrString,
    callback: (inCall: boolean, isSos: boolean) => void
  ) {
    Redis.getActiveCall(userId, (err, ac) => {
      if (!err && ac) {
        return callback(true, ac.isSos);
      }
      const uid = userId + "";
      const groups = (activeCallGroupsOfUsersSet[uid] || []);
      return callback(groups.length > 0, false);
    });
  }

  /**
   * Returns authoritative call details for a user to decide on overlap rejection/override.
   *
   * In-memory group state is always checked first — it is updated synchronously on every
   * join/leave so it is always current. Redis is only consulted for private-call state
   * (where there is no in-memory equivalent). This avoids stale Redis TTL entries from
   * incorrectly blocking group members after they have already left a call.
   */
  public static getCallDetailsForUser(
    userId: numberOrString,
    callback: (err: Error, details: { inCall: boolean; channelType: number; targetId: string; isSos: boolean }) => void
  ) {
    const uid = userId + "";

    // 1. In-memory group state — always authoritative (updated synchronously on join/leave).
    const groups = (activeCallGroupsOfUsersSet[uid] || []);
    if (groups.length > 0) {
      const gs = userGroupCallState[uid];
      return callback(null, { inCall: true, channelType: 2, targetId: groups[0] + "", isSos: gs ? gs.isSos : false });
    }

    // 2. In-memory private call state — reliable even when Redis is unavailable.
    const ps = userPrivateCallState[uid];
    if (ps) {
      return callback(null, { inCall: true, channelType: 1, targetId: ps.peerId, isSos: ps.isSos });
    }

    // 3. Redis fallback — catches calls established on a different worker process
    //    (multi-process cluster deployments) that aren't in this process's memory.
    Redis.getActiveCall(userId, (err, ac) => {
      if (err) {
        return callback(null, { inCall: false, channelType: 0, targetId: "", isSos: false });
      }
      if (ac && (ac.channelType === 1 || ac.channelType === 2)) {
        // For group calls: validate against in-memory state.  If in-memory shows no active
        // group but Redis says inCall=true, the Redis entry is stale (server restart while
        // the call was active, or CallEndedForAll arrived before the key was cleared).
        // Clear it so subsequent calls are not incorrectly rejected as "busy".
        if (ac.channelType === 2) {
          const userGroups = (activeCallGroupsOfUsersSet[uid] || []);
          if (userGroups.length === 0) {
            Redis.clearActiveCall(userId);
            return callback(null, { inCall: false, channelType: 0, targetId: "", isSos: false });
          }
        }
        // For private calls: validate against in-memory state and check for invalid targets.
        // If Redis says in a private call with target "00000" or similar invalid ID, the entry
        // is stale (disconnected peer or cleanup failure). Clear it so calls can proceed.
        if (ac.channelType === 1) {
          const privateCallState = userPrivateCallState[uid];
          const targetId = (ac.targetId || "").toString();

          // Check if target is invalid (00000, 0, or empty) or in-memory state doesn't match
          if (!targetId || targetId === "00000" || targetId === "0" ||
              !privateCallState || privateCallState.peerId.toString() !== targetId) {
            Redis.clearActiveCall(userId);
            // Also clear in-memory state if it exists
            if (privateCallState) {
              delete userPrivateCallState[uid];
            }
            return callback(null, { inCall: false, channelType: 0, targetId: "", isSos: false });
          }
        }
        return callback(null, {
          channelType: ac.channelType,
          inCall: true,
          isSos: !!ac.isSos,
          targetId: (ac.targetId || "") + ""
        });
      }
      return callback(null, { inCall: false, channelType: 0, targetId: "", isSos: false });
    });
  }

  /**
   * Reverses the effect of addUserToActiveCallGroup + setUserGroupCallState when a group
   * call is rejected before delivery (e.g. all members busy). Unlike removeUserFromActiveCallGroup,
   * this does NOT mark the user as a "dropped participant" since they never actually joined.
   */
  public static cancelGroupStart(userId: numberOrString, groupId: numberOrString) {
    const uid = userId + "";
    const gid = groupId + "";
    activeCallGroupsOfUsersSet[uid] = (activeCallGroupsOfUsersSet[uid] || [])
      .map((id) => id + "")
      .filter((id) => id !== gid);
    if (userGroupCallState[uid] && userGroupCallState[uid].groupId === gid) {
      delete userGroupCallState[uid];
    }
    Redis.clearActiveCall(uid);
    Redis.removeActiveGroupForUser(uid, groupId);
    States.removeActiveParticipantFromGroup(groupId, userId);
  }

  /** Set the subset of recipients that actually received START for the current group floor session. */
  public static setGroupFloorRecipients(
    groupId: numberOrString,
    ownerId: numberOrString,
    recipients: numberOrString[]
  ): number {
    const gid = groupId + "";
    const epoch = (groupFloorEpochById[gid] || 0) + 1;
    groupFloorEpochById[gid] = epoch;
    groupFloorRecipients[gid] = {
      ownerId: ownerId + "",
      recipients: recipients.map((r) => r + ""),
      sessionEpoch: epoch
    };
    return epoch;
  }

  /** Returns the recipient subset, or undefined if there is no restriction (normal call). */
  public static getGroupFloorRecipients(
    groupId: numberOrString,
    ownerId?: numberOrString
  ): string[] | undefined {
    const session = groupFloorRecipients[groupId + ""];
    if (!session) { return undefined; }
    if (ownerId !== undefined && session.ownerId !== ownerId + "") { return undefined; }
    return session.recipients;
  }

  /** Clear recipient restriction when the floor is released. */
  public static clearGroupFloorRecipients(
    groupId: numberOrString,
    ownerId?: numberOrString,
    sessionEpoch?: number
  ): void {
    const gid = groupId + "";
    const session = groupFloorRecipients[gid];
    if (!session) { return; }
    if (ownerId !== undefined && session.ownerId !== ownerId + "") { return; }
    if (sessionEpoch !== undefined && session.sessionEpoch !== sessionEpoch) { return; }
    delete groupFloorRecipients[gid];
  }

  public static setGroupSos(groupId: numberOrString, isSos: boolean) {
    groupSosState[groupId + ""] = isSos;
  }

  public static isGroupSos(groupId: numberOrString): boolean {
    return groupSosState[groupId + ""] === true;
  }

  public static releaseFloorOwnershipForUser(
    userId: numberOrString,
    callback?: (err: Error, releasedGroups: Array<number|string>) => void
  ) {
    userId = userId + "";
    const userIdStr = userId + "";
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
    const ownedGroupFloors = groupFloorKeysByUser[userIdStr];
    if (ownedGroupFloors && ownedGroupFloors.size > 0) {
      ownedGroupFloors.forEach((groupId) => {
        Redis.releaseGroupFloor(groupId, userIdStr, () => {
          States.clearGroupFloorRecipients(groupId, userIdStr);
          States.removeBusyStateOfGroup(groupId);
        });
      });
      delete groupFloorKeysByUser[userIdStr];
    }
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
    const groupIdStr = groupId + "";
    const userIdStr = userId + "";
    // Use atomic Redis SET NX EX — only ONE worker across the cluster can win.
    Redis.acquireGroupFloor(groupIdStr, userIdStr, GROUP_FLOOR_TTL_SEC, (err, acquired, currentOwner) => {
      if (err) { return callback(err, false, 0); }
      if (acquired) {
        if (!groupFloorKeysByUser[userIdStr]) { groupFloorKeysByUser[userIdStr] = new Set(); }
        groupFloorKeysByUser[userIdStr].add(groupIdStr);
        // Mirror into local message state so the rest of the code (audioTime
        // inspection, floor-owner checks) keeps working as before.
        States.setBusyStateOfGroup(groupIdStr, userIdStr, (setErr) => {
          if (setErr) {
            // Roll back the Redis lock so we don't leave a phantom floor.
            Redis.releaseGroupFloor(groupIdStr, userIdStr);
            groupFloorKeysByUser[userIdStr].delete(groupIdStr);
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
      Redis.refreshGroupFloor(groupId, userId + "", GROUP_FLOOR_TTL_SEC, (refreshErr) => {
        if (refreshErr) {
          if (callback) { return callback(refreshErr, false); }
          return;
        }
        States.updateAudioTimeOfGroup(groupId, callback);
      });
    });
  }

  public static releaseFloorOfGroup(
    groupId: numberOrString,
    userId: numberOrString,
    callback?: (err: Error, released: boolean) => void
  ) {
    groupId = groupId + "";
    userId = userId + "";
    const groupIdStr = groupId + "";
    const userIdStr = userId + "";
    // Atomically release the Redis lock (owner-only Lua script), then clear
    // the local mirror regardless so stale in-memory state doesn't linger.
    Redis.releaseGroupFloor(groupIdStr, userIdStr, (redisErr, released) => {
      if (redisErr) {
        debug(`releaseFloorOfGroup Redis error groupId:${groupId} userId:${userId} err:${redisErr}`);
      }
      if (groupFloorKeysByUser[userIdStr]) {
        groupFloorKeysByUser[userIdStr].delete(groupIdStr);
        if (groupFloorKeysByUser[userIdStr].size === 0) {
          delete groupFloorKeysByUser[userIdStr];
        }
      }
      // Clear local mirror unconditionally — even if redis said "not owner"
      // (e.g. TTL expired) we still want to clean up local state.
      States.removeBusyStateOfGroup(groupIdStr, function(removeErr) {
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
   *
   * Fast path: consult the in-memory privateFloorKeysByUser index (updated
   * synchronously on acquire/release within this worker — zero Redis latency).
   * Fallback: plain Redis GET so the check is accurate in multi-worker clusters
   * where the floor was acquired by a different process.
   *
   * Previously this called acquirePrivateFloor (SET NX EX) which accidentally
   * locked the floor on every audio-packet check and had to release it immediately,
   * adding 2 unnecessary Redis round-trips per packet.
   */
  public static isPrivateFloorOwner(
    userId1: numberOrString,
    userId2: numberOrString,
    userId: numberOrString,
    callback: (err: Error, isOwner: boolean) => void
  ): void {
    const key = privateFloorKey(userId1, userId2);
    const userIdStr = userId + "";

    // In-memory fast path — no Redis I/O needed within the same worker process.
    if (privateFloorKeysByUser[userIdStr] && privateFloorKeysByUser[userIdStr].has(key)) {
      return callback(null, true);
    }

    // Cluster fallback: read the key without touching it.
    Redis.getPrivateFloorOwner(key, (err, owner) => {
      if (err) { return callback(err, false); }
      return callback(null, owner === userIdStr);
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
          const audioTime = message.audioTime;
          if (!!userId && !!audioTime) {
            const silenceDuration = Date.now() - audioTime;
            if (silenceDuration > GROUPS_BUSY_TIMEOUT) {
              // Properly wipe the Redis lock and local state instead of just local memory
              States.releaseFloorOfGroup(groupId, userId);
              debug(`GROUPS_BUSY_TIMEOUT userId: ${userId} silent for ${silenceDuration}ms` +
                    ` (> ${GROUPS_BUSY_TIMEOUT}), channel is no longer busy`);
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
