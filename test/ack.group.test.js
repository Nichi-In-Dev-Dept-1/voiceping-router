/* eslint-disable func-names */
"use strict";

var chai = require("chai");
var notepack = require("notepack");
var Q = require("q");
var util = require("util");

var connectClientPromisified = require("./client-promise");
var runServerPromisified = require("./server-promise");
var VP = require("../dist/lib/voiceping");

var expect = chai.expect;

var ChannelType = VP.ChannelType;
var MessageType = VP.MessageType;

var port = 3334;

function waitForMessageType(ws, expectedType, onMatch) {
  function handler(data) {
    var decoded = notepack.decode(data);
    if (decoded[1] !== expectedType) {
      ws.once("message", handler);
      return;
    }
    onMatch(data, decoded);
  }

  ws.once("message", handler);
}

describe("router group messaging busy state flows", function() {
  var server = null;
  var userId1 = "1";
  var userId2 = "2";
  var userId3 = "3";
  var groupId = "18";
  var ws1 = null;
  var ws2 = null;
  var ws3 = null;

  before(function(done) {
    runServerPromisified(port)
      .then(function(server1) {
        server = server1;

        return Q.all([
          connectClientPromisified(port, userId1, groupId),
          connectClientPromisified(port, userId2, groupId),
          connectClientPromisified(port, userId3, groupId)
        ]).then((wss) => {
          ws1 = wss[0];
          ws2 = wss[1];
          ws3 = wss[2];
          setTimeout(function() {
            done();
          }, 1000);
        }).catch((err) => {
          done(err);
        });
      }).catch((err) => {
        done(err);
      });
  });

  it("should receive acknowledge start for sender and start talking for receiver", function(done) {
    var from = userId1;
    var to = groupId;

    var timestamp = Date.now();
    var message = notepack.encode([ChannelType.GROUP, MessageType.START, from, to, timestamp]);
    ws1.send(message);

    var acknowledged = false;
    var received = false;

    ws1.once("message", function(data) {
      expect(data).to.not.be.empty;

      var decoded = notepack.decode(data);
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.START_ACK);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;
      expect(payload).to.be.equal(`${timestamp}`);

      acknowledged = true;
      if (received) { done(); }
    });

    waitForMessageType(ws2, MessageType.START, function(data, decoded) {
      expect(data).to.not.be.empty;
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.START);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;
      expect(payload).to.be.equal(timestamp);

      received = true;
      if (acknowledged) { done(); }
    });
  });

  it("should broadcast audio message to group", function(done) {
    var from = userId1;
    var to = groupId;

    function whitenoise() {
      var bufferSize = 4096;
      var out = [[], []];
      for (var i = 0; i < bufferSize; i++) {
        out[0][i] = [1][i] = Math.random() * 0.25;
      }
      return new Buffer(out);
    }

    var audiobuffer = whitenoise();
    var message = notepack.encode([ChannelType.GROUP, MessageType.AUDIO, from, to, audiobuffer]);
    ws1.send(message);

    var received1 = false;
    var received2 = false;

    waitForMessageType(ws2, MessageType.AUDIO, function(data, decoded) {
      expect(data).to.not.be.empty;
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.AUDIO);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;

      received1 = true;
      if (received2) { done(); }
    });

    ws3.once("message", function(data) {
      expect(data).to.not.be.empty;

      var decoded = notepack.decode(data);
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.AUDIO);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;

      received2 = true;
      if (received1) { done(); }
    });
  });

  it("should receive acknowledge start failed for 2nd sender", function(done) {
    var from = userId2;
    var to = groupId;
    var timestamp = Date.now();

    var message = notepack.encode([ChannelType.GROUP, MessageType.START, from, to, timestamp]);
    ws2.send(message);

    waitForMessageType(ws2, MessageType.START_FAILED, function(data, decoded) {
      expect(data).to.not.be.empty;
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.START_FAILED);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;
      expect(payload).to.be.equal("Busy");

      done();
    });
  });

  it("should drop audio from non owner while floor is busy", function(done) {
    var from = userId2;
    var to = groupId;

    function whitenoise() {
      var bufferSize = 512;
      var out = [[], []];
      for (var i = 0; i < bufferSize; i++) {
        out[0][i] = [1][i] = Math.random() * 0.25;
      }
      return new Buffer(out);
    }

    var audiobuffer = whitenoise();
    var message = notepack.encode([ChannelType.GROUP, MessageType.AUDIO, from, to, audiobuffer]);

    var unexpected = function() {
      done(new Error("non-owner audio should not be broadcast"));
    };

    ws2.once("message", unexpected);
    ws3.once("message", unexpected);

    ws2.send(message);

    setTimeout(function() {
      ws2.removeListener("message", unexpected);
      ws3.removeListener("message", unexpected);
      done();
    }, 300);
  });

  it("should ignore stop from non owner while current owner is talking", function(done) {
    var from = userId2;
    var to = groupId;
    var timestamp = Date.now();

    ws2.send(notepack.encode([ChannelType.GROUP, MessageType.STOP, from, to, timestamp]));

    setTimeout(function() {
      ws2.send(notepack.encode([ChannelType.GROUP, MessageType.START, from, to, Date.now()]));

      waitForMessageType(ws2, MessageType.START_FAILED, function(data) {
        var decoded = notepack.decode(data);
        expect(decoded[0]).to.be.equal(ChannelType.GROUP);
        expect(decoded[1]).to.be.equal(MessageType.START_FAILED);
        expect(decoded[4]).to.be.equal("Busy");
        done();
      });
    }, 150);
  });

  it("should allow only one winner during a simultaneous start burst", function(done) {
    var contenders = [ws1, ws2, ws3];
    var userIds = [userId1, userId2, userId3];
    var ackCount = 0;
    var failCount = 0;
    var expectedFailures = contenders.length - 1;
    var totalResults = contenders.length;
    var receivedResults = 0;

    function onResult(decoded) {
      if (decoded[1] === MessageType.START_ACK) {
        ackCount += 1;
      } else if (decoded[1] === MessageType.START_FAILED) {
        failCount += 1;
      }

      receivedResults += 1;

      if (receivedResults === totalResults) {
        expect(ackCount).to.be.equal(1);
        expect(failCount).to.be.equal(expectedFailures);
        contenders.forEach(function(ws) {
          ws.removeListener("message", onMessage);
        });
        done();
      }
    }

    function onMessage(data) {
      var decoded = notepack.decode(data);
      if (decoded[1] !== MessageType.START_ACK && decoded[1] !== MessageType.START_FAILED) {
        return;
      }
      onResult(decoded);
    }

    contenders.forEach(function(ws, index) {
      ws.on("message", onMessage);

      ws.send(notepack.encode([
        ChannelType.GROUP,
        MessageType.START,
        userIds[index],
        groupId,
        Date.now() + index
      ]));
    });
  });

  it("should receive acknowledge stop for sender and stop talking for receiver", function(done) {
    var from = userId1;
    var to = groupId;

    // timestamp on stop talking is not implemented on mobile clients,
    // this is just a test payload data
    var timestamp = Date.now();
    var message = notepack.encode([ChannelType.GROUP, MessageType.STOP, from, to, timestamp]);
    ws1.send(message);

    var acknowledged = false;
    var received = false;

    ws1.once("message", function(data) {
      expect(data).to.not.be.empty;

      var decoded = notepack.decode(data);
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.STOP_ACK);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;

      var messageId = util.format("%d_%d_%d_%d", ChannelType.GROUP, MessageType.AUDIO, to, from);
      var re = new RegExp("^" + messageId);
      expect(payload).to.match(re);

      acknowledged = true;
      if (received) { done(); }
    });

    ws2.once("message", function(data) {
      expect(data).to.not.be.empty;

      var decoded = notepack.decode(data);
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.STOP);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;

      var content = JSON.parse(payload);
      expect(content.message_id).to.not.be.empty;

      var messageId = util.format("%d_%d_%d_%d", ChannelType.GROUP, MessageType.AUDIO, to, from);
      var re = new RegExp("^" + messageId);
      expect(content.message_id).to.match(re);

      received = true;
      if (acknowledged) { done(); }
    });
  });

  it("should receive acknowledge start for sender and start talking for receiver", function(done) {
    var from = userId2;
    var to = groupId;

    var timestamp = Date.now();
    var message = notepack.encode([ChannelType.GROUP, MessageType.START, from, to, timestamp]);
    ws2.send(message);

    var acknowledged = false;
    var received = false;

    ws2.once("message", function(data) {
      expect(data).to.not.be.empty;

      var decoded = notepack.decode(data);
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.START_ACK);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;
      expect(payload).to.be.equal(`${timestamp}`);

      acknowledged = true;
      if (received) { done(); }
    });

    waitForMessageType(ws3, MessageType.START, function(data, decoded) {
      expect(data).to.not.be.empty;
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.START);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;
      expect(payload).to.be.equal(timestamp);

      received = true;
      if (acknowledged) { done(); }
    });
  });

  it("should broadcast audio message to group", function(done) {
    var from = userId2;
    var to = groupId;

    function whitenoise() {
      var bufferSize = 4096;
      var out = [[], []];
      for (var i = 0; i < bufferSize; i++) {
        out[0][i] = [1][i] = Math.random() * 0.25;
      }
      return new Buffer(out);
    }

    var audiobuffer = whitenoise();
    var message = notepack.encode([ChannelType.GROUP, MessageType.AUDIO, from, to, audiobuffer]);
    ws2.send(message);

    var received1 = false;
    var received2 = false;

    waitForMessageType(ws1, MessageType.AUDIO, function(data, decoded) {
      expect(data).to.not.be.empty;
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.AUDIO);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;

      received1 = true;
      if (received2) { done(); }
    });

    waitForMessageType(ws3, MessageType.AUDIO, function(data, decoded) {
      expect(data).to.not.be.empty;
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.AUDIO);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;

      received2 = true;
      if (received1) { done(); }
    });
  });

  it("should receive acknowledge start failed for 2nd sender", function(done) {
    var from = userId1;
    var to = groupId;
    var timestamp = Date.now();

    var message = notepack.encode([ChannelType.GROUP, MessageType.START, from, to, timestamp]);
    ws1.send(message);

    ws1.once("message", function(data) {
      expect(data).to.not.be.empty;

      var decoded = notepack.decode(data);
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.START_FAILED);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;
      expect(payload).to.be.equal("Busy");

      done();
    });
  });

  it("should receive acknowledge stop for sender and stop talking for receiver", function(done) {
    var from = userId2;
    var to = groupId;

    // timestamp on stop talking is not implemented on mobile clients,
    // this is just a test payload data
    var timestamp = Date.now();
    var message = notepack.encode([ChannelType.GROUP, MessageType.STOP, from, to, timestamp]);
    ws2.send(message);

    var acknowledged = false;
    var received = false;

    ws2.once("message", function(data) {
      expect(data).to.not.be.empty;

      var decoded = notepack.decode(data);
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.STOP_ACK);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;

      var messageId = util.format("%d_%d_%d_%d", ChannelType.GROUP, MessageType.AUDIO, to, from);
      var re = new RegExp("^" + messageId);
      expect(payload).to.match(re);

      acknowledged = true;
      if (received) { done(); }
    });

    ws3.once("message", function(data) {
      expect(data).to.not.be.empty;

      var decoded = notepack.decode(data);
      var channelType = decoded[0];
      expect(channelType).to.be.equal(ChannelType.GROUP);

      var messageType = decoded[1];
      expect(messageType).to.be.equal(MessageType.STOP);

      var payload = decoded[4];
      expect(payload).to.not.be.empty;

      var content = JSON.parse(payload);
      expect(content.message_id).to.not.be.empty;

      var messageId = util.format("%d_%d_%d_%d", ChannelType.GROUP, MessageType.AUDIO, to, from);
      var re = new RegExp("^" + messageId);
      expect(content.message_id).to.match(re);

      received = true;
      if (acknowledged) { done(); }
    });
  });

  after(function() {
    ws1.send(notepack.encode([ChannelType.GROUP, MessageType.STOP, userId1, groupId]));
    ws2.send(notepack.encode([ChannelType.GROUP, MessageType.STOP, userId2, groupId]));
    ws1.close();
    ws2.close();
    ws3.close();
    server.close();
  });
});
