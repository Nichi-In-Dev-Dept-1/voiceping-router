/* eslint-disable func-names */
"use strict";

var chai = require("chai");
var expect = chai.expect;
var notepack = require("notepack");
var Q = require("q");

var connectClientPromisified = require("./client-promise");
var runServerPromisified = require("./server-promise");

var ChannelType = require("../dist/lib/channeltype");
var MessageType = require("../dist/lib/messagetype");

var port = 3344;

function waitForMessage(ws, label, predicate, timeoutMs) {
  return new Promise(function(resolve, reject) {
    var timeout = setTimeout(function() {
      ws.removeListener("message", onMessage);
      reject(new Error("Timed out waiting for matching websocket message: " + label));
    }, timeoutMs || 5000);

    function onMessage(data) {
      var decoded = notepack.decode(data);
      try {
        if (!predicate(decoded)) { return; }
        clearTimeout(timeout);
        ws.removeListener("message", onMessage);
        resolve(decoded);
      } catch (err) {
        clearTimeout(timeout);
        ws.removeListener("message", onMessage);
        reject(err);
      }
    }

    ws.on("message", onMessage);
  });
}

describe("router interrupt group floor takeover", function() {
  var server = null;
  var userId1 = "101";
  var userId2 = "102";
  var userId3 = "103";
  var groupId = "118";
  var ws1 = null;
  var ws2 = null;
  var ws3 = null;

  before(function(done) {
    this.timeout(10000);
    runServerPromisified(port)
      .then(function(server1) {
        server = server1;

        return Q.all([
          connectClientPromisified(port, userId1, groupId),
          connectClientPromisified(port, userId2, groupId),
          connectClientPromisified(port, userId3, groupId)
        ]).then(function(wss) {
          ws1 = wss[0];
          ws2 = wss[1];
          ws3 = wss[2];
          setTimeout(function() {
            done();
          }, 1000);
        });
      })
      .catch(done);
  });

  it("forces a STOP for the current floor owner before granting floor to the interruptor", function() {
    this.timeout(10000);
    var interruptPayload = null;
    var ownerStartPayload = Date.now().toString();
    var initialStart = Promise.all([
      waitForMessage(ws1, "owner receives initial START_ACK", function(decoded) {
        return decoded[1] === MessageType.START_ACK && decoded[2] === userId1 && decoded[3] === groupId;
      }),
      waitForMessage(ws2, "user2 sees owner START", function(decoded) {
        return decoded[1] === MessageType.START && decoded[2] === userId1 && decoded[3] === groupId;
      }),
      waitForMessage(ws3, "user3 sees owner START", function(decoded) {
        return decoded[1] === MessageType.START && decoded[2] === userId1 && decoded[3] === groupId;
      })
    ]);

    ws1.send(notepack.encode([ChannelType.GROUP, MessageType.START, userId1, groupId, ownerStartPayload]));

    return initialStart.then(function() {
      interruptPayload = JSON.stringify({ isInterrupt: true });
      var interruptStart = Promise.all([
        waitForMessage(ws2, "interruptor receives START_ACK", function(decoded) {
          return decoded[1] === MessageType.START_ACK && decoded[2] === userId2 && decoded[3] === groupId;
        }),
        waitForMessage(ws1, "owner receives STOP_ACK", function(decoded) {
          return decoded[1] === MessageType.STOP_ACK && decoded[2] === userId1 && decoded[3] === groupId;
        }),
        waitForMessage(ws1, "owner receives interrupt START", function(decoded) {
          return decoded[1] === MessageType.START && decoded[2] === userId2 && decoded[3] === groupId;
        }),
        waitForMessage(ws3, "member receives owner STOP", function(decoded) {
          return decoded[1] === MessageType.STOP && decoded[2] === userId1 && decoded[3] === groupId;
        }),
        waitForMessage(ws3, "member receives interrupt START", function(decoded) {
          return decoded[1] === MessageType.START && decoded[2] === userId2 && decoded[3] === groupId;
        })
      ]);

      ws2.send(notepack.encode([ChannelType.GROUP, MessageType.START, userId2, groupId, interruptPayload]));
      return interruptStart;
    }).then(function(results) {
      var interruptAck = results[0];
      var ownerStopAck = results[1];
      var ownerIncomingStart = results[2];
      var memberStop = results[3];
      var memberIncomingStart = results[4];
      var interruptAckPayload = JSON.parse(interruptAck[4]);
      var ownerIncomingStartPayload = JSON.parse(ownerIncomingStart[4]);
      var memberIncomingStartPayload = JSON.parse(memberIncomingStart[4]);

      expect(interruptAckPayload.isInterrupt).to.equal(true);
      expect(ownerStopAck[4]).to.not.be.empty;
      expect(ownerIncomingStartPayload.isInterrupt).to.equal(true);

      var stopPayload = JSON.parse(memberStop[4]);
      expect(stopPayload.message_id).to.not.be.empty;

      expect(memberIncomingStartPayload.isInterrupt).to.equal(true);
    });
  });

  after(function() {
    try {
      ws2.send(notepack.encode([ChannelType.GROUP, MessageType.STOP, userId2, groupId]));
    } catch (e) { /* ignore */ }
    ws1.close();
    ws2.close();
    ws3.close();
    server.close();
  });
});
