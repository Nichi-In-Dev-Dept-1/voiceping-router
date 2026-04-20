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

var port = 8011;

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

describe("router interrupt private floor takeover", function() {
  var server = null;
  var userId1 = null;
  var userId2 = null;
  var userId3 = null;
  var ws1 = null;
  var ws2 = null;
  var ws3 = null;

  before(function(done) {
    this.timeout(10000);
    var runId = Date.now().toString().slice(-6);
    userId1 = "201" + runId;
    userId2 = "202" + runId;
    userId3 = "203" + runId;
    runServerPromisified(port)
      .then(function(server1) {
        server = server1;

        return Q.all([
          connectClientPromisified(port, userId1),
          connectClientPromisified(port, userId2),
          connectClientPromisified(port, userId3)
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

  it("forces a STOP for the current private floor owner before granting floor to the interruptor", function() {
    this.timeout(10000);

    var ownerStartPayload = Date.now().toString();
    var initialStart = Promise.all([
      waitForMessage(ws1, "owner receives initial START_ACK", function(decoded) {
        return decoded[1] === MessageType.START_ACK && decoded[2] === userId1 && decoded[3] === userId2;
      }),
      waitForMessage(ws2, "peer receives initial START", function(decoded) {
        return decoded[1] === MessageType.START && decoded[2] === userId1 && decoded[3] === userId2;
      })
    ]);

    ws1.send(notepack.encode([ChannelType.PRIVATE, MessageType.START, userId1, userId2, ownerStartPayload]));

    return initialStart.then(function() {
      var interruptPayload = JSON.stringify({ isInterrupt: true });
      var interruptStart = Promise.all([
        waitForMessage(ws1, "owner receives STOP_ACK", function(decoded) {
          return decoded[1] === MessageType.STOP_ACK && decoded[2] === userId1 && decoded[3] === userId2;
        }),
        waitForMessage(ws1, "owner receives interrupt START", function(decoded) {
          return decoded[1] === MessageType.START && decoded[2] === userId2 && decoded[3] === userId1;
        }),
        waitForMessage(ws2, "interruptor receives owner STOP", function(decoded) {
          return decoded[1] === MessageType.STOP && decoded[2] === userId1 && decoded[3] === userId2;
        }),
        waitForMessage(ws2, "interruptor receives START_ACK", function(decoded) {
          return decoded[1] === MessageType.START_ACK && decoded[2] === userId2 && decoded[3] === userId1;
        })
      ]);

      ws2.send(notepack.encode([ChannelType.PRIVATE, MessageType.START, userId2, userId1, interruptPayload]));

      return interruptStart.then(function(results) {
        var ownerStopAck = results[0];
        var ownerIncomingStart = results[1];
        var interruptorIncomingStop = results[2];
        var interruptAck = results[3];

        expect(ownerStopAck[4]).to.not.be.empty;

        var ownerIncomingPayload = JSON.parse(ownerIncomingStart[4]);
        expect(ownerIncomingPayload.isInterrupt).to.equal(true);

        var interruptorStopPayload = JSON.parse(interruptorIncomingStop[4]);
        expect(interruptorStopPayload.message_id).to.not.be.empty;

        var interruptAckPayload = JSON.parse(interruptAck[4]);
        expect(interruptAckPayload.isInterrupt).to.equal(true);
      });
    });
  });

  after(function() {
    try {
      ws2.send(notepack.encode([ChannelType.PRIVATE, MessageType.STOP, userId2, userId1, Date.now()]));
    } catch (e) { /* ignore */ }
    ws1.close();
    ws2.close();
    ws3.close();
    server.close();
  });
});
