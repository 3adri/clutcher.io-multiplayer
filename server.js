const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 10000);

const wss = new WebSocket.Server({
  server
});

// =========================
// CONFIG
// =========================

const MAX_PLAYERS = 32;
const MAX_NAME = 24;
const MAX_CHAT = 300;
const MAX_ROOMS = 100;

// =========================
// DATA
// =========================

const rooms = new Map();
const clients = new Map();

// =========================
// UTILS
// =========================

function randomId(prefix) {
  return `${prefix}_${crypto.randomBytes(6).toString("hex")}`;
}

function cleanText(value, maxLength) {
  if (typeof value !== "string") {
    return "";
  }

  return value
    .replace(/[<>]/g, "")
    .trim()
    .slice(0, maxLength);
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(room, data, except = null) {
  if (!room) return;

  for (const playerId of room.players) {
    const player = clients.get(playerId);

    if (!player || player.ws === except) {
      continue;
    }

    send(player.ws, data);
  }
}

function getPlayerData(player) {
  return {
    id: player.id,
    name: player.name,
    state: player.state
  };
}

function getRoomData(room) {
  return {
    id: room.id,
    name: room.name,
    maxPlayers: room.maxPlayers,
    teamSize: room.teamSize,
    players: room.players.size
  };
}

function sendRooms(ws) {
  send(ws, {
    type: "rooms",
    rooms: Array.from(rooms.values()).map(getRoomData)
  });
}

// =========================
// ROOM MANAGEMENT
// =========================

function removeFromRoom(player, notify = true) {
  if (!player.roomId) {
    return;
  }

  const room = rooms.get(player.roomId);
  const oldRoomId = player.roomId;

  player.roomId = null;

  if (!room) {
    if (notify) {
      send(player.ws, {
        type: "left_room",
        roomId: oldRoomId
      });
    }

    return;
  }

  room.players.delete(player.id);

  if (notify) {
    send(player.ws, {
      type: "left_room",
      roomId: oldRoomId
    });
  }

  broadcast(room, {
    type: "player_left",
    id: player.id
  });

  if (room.players.size === 0) {
    rooms.delete(room.id);
  }
}

// =========================
// HTTP
// =========================

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "clutcher-io-online",
    websocket: true,
    rooms: rooms.size,
    clients: clients.size
  });
});

app.get("/health", (req, res) => {
  res.json({
    ok: true
  });
});

// =========================
// WEBSOCKET
// =========================

wss.on("connection", (ws) => {
  const id = randomId("player");

  const player = {
    id,
    ws,
    name: `Player_${id.slice(-4)}`,
    roomId: null,

    state: {
      x: 0,
      y: 0,
      z: 0,

      vx: 0,
      vy: 0,
      vz: 0,

      yaw: 0,
      pitch: 0,

      alive: true
    }
  };

  clients.set(id, player);

  // -------------------------
  // CONNECTION
  // -------------------------

  send(ws, {
    type: "connected",
    id
  });

  // -------------------------
  // MESSAGE
  // -------------------------

  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch {
      send(ws, {
        type: "error",
        message: "Invalid JSON"
      });

      return;
    }

    if (!data || typeof data.type !== "string") {
      send(ws, {
        type: "error",
        message: "Invalid message"
      });

      return;
    }

    // =======================
    // HELLO
    // =======================

    if (data.type === "hello") {
      send(ws, {
        type: "hello",
        id: player.id
      });

      send(ws, {
        type: "connected",
        id: player.id
      });

      sendRooms(ws);

      return;
    }

    // =======================
    // PING
    // =======================

    if (data.type === "ping") {
      send(ws, {
        type: "pong",
        t: data.t ?? Date.now()
      });

      return;
    }

    // =======================
    // LIST ROOMS
    // =======================

    if (data.type === "list_rooms") {
      sendRooms(ws);
      return;
    }

    // =======================
    // SET NAME
    // =======================

    if (data.type === "set_name") {
      const name = cleanText(data.name, MAX_NAME);

      if (name) {
        player.name = name;
      }

      if (player.roomId) {
        const room = rooms.get(player.roomId);

        if (room) {
          broadcast(room, {
            type: "player_state",
            player: getPlayerData(player)
          });
        }
      }

      return;
    }

    // =======================
    // CREATE ROOM
    // =======================

    if (data.type === "create_room") {
      if (rooms.size >= MAX_ROOMS) {
        send(ws, {
          type: "error",
          message: "Maximum number of rooms reached"
        });

        return;
      }

      const name =
        cleanText(data.name, 50) ||
        `Room ${rooms.size + 1}`;

      let maxPlayers = Number(data.maxPlayers);

      if (!Number.isFinite(maxPlayers)) {
        maxPlayers = MAX_PLAYERS;
      }

      maxPlayers = Math.max(
        1,
        Math.min(MAX_PLAYERS, Math.floor(maxPlayers))
      );

      let teamSize = Number(data.teamSize);

      if (!Number.isFinite(teamSize)) {
        teamSize = 1;
      }

      teamSize = Math.max(
        1,
        Math.min(maxPlayers, Math.floor(teamSize))
      );

      const room = {
        id: randomId("room"),
        name,
        maxPlayers,
        teamSize,
        players: new Set()
      };

      rooms.set(room.id, room);

      // El creador entra automáticamente
      if (player.roomId) {
        removeFromRoom(player);
      }

      room.players.add(player.id);
      player.roomId = room.id;

      send(ws, {
        type: "room_joined",
        roomId: room.id,
        room: getRoomData(room),
        players: Array.from(room.players)
          .map((id) => clients.get(id))
          .filter(Boolean)
          .map(getPlayerData)
      });

      sendRooms(ws);

      return;
    }

    // =======================
    // JOIN ROOM
    // =======================

    if (data.type === "join_room") {
      const roomId = String(data.roomId || "");
      const room = rooms.get(roomId);

      if (!room) {
        send(ws, {
          type: "error",
          message: "Room not found"
        });

        return;
      }

      if (room.players.size >= room.maxPlayers) {
        send(ws, {
          type: "error",
          message: "Room is full"
        });

        return;
      }

      if (player.roomId === room.id) {
        send(ws, {
          type: "room_joined",
          roomId: room.id,
          room: getRoomData(room),
          players: Array.from(room.players)
            .map((id) => clients.get(id))
            .filter(Boolean)
            .map(getPlayerData)
        });

        return;
      }

      if (player.roomId) {
        removeFromRoom(player);
      }

      room.players.add(player.id);
      player.roomId = room.id;

      send(ws, {
        type: "room_joined",
        roomId: room.id,
        room: getRoomData(room),
        players: Array.from(room.players)
          .map((id) => clients.get(id))
          .filter(Boolean)
          .map(getPlayerData)
      });

      broadcast(
        room,
        {
          type: "player_joined",
          player: getPlayerData(player)
        },
        ws
      );

      broadcast(room, {
        type: "room_update",
        room: getRoomData(room),
        players: Array.from(room.players)
          .map((id) => clients.get(id))
          .filter(Boolean)
          .map(getPlayerData)
      });

      return;
    }

    // =======================
    // LEAVE ROOM
    // =======================

    if (data.type === "leave_room") {
      removeFromRoom(player);
      sendRooms(ws);
      return;
    }

    // =======================
    // PLAYER STATE
    // =======================

    if (data.type === "player_state") {
      if (!player.roomId) {
        return;
      }

      const room = rooms.get(player.roomId);

      if (!room) {
        return;
      }

      if (!data.state || typeof data.state !== "object") {
        return;
      }

      const state = data.state;

      const numeric = [
        "x",
        "y",
        "z",
        "vx",
        "vy",
        "vz",
        "yaw",
        "pitch"
      ];

      for (const key of numeric) {
        if (typeof state[key] === "number" && Number.isFinite(state[key])) {
          player.state[key] = state[key];
        }
      }

      if (typeof state.alive === "boolean") {
        player.state.alive = state.alive;
      }

      broadcast(
        room,
        {
          type: "player_state",
          player: getPlayerData(player)
        },
        ws
      );

      return;
    }

    // =======================
    // CHAT
    // =======================

    if (data.type === "chat") {
      if (!player.roomId) {
        return;
      }

      const room = rooms.get(player.roomId);

      if (!room) {
        return;
      }

      const message = cleanText(data.message, MAX_CHAT);

      if (!message) {
        return;
      }

      broadcast(room, {
        type: "chat",
        id: player.id,
        name: player.name,
        message
      });

      return;
    }

    // =======================
    // UNKNOWN MESSAGE
    // =======================

    send(ws, {
      type: "error",
      message: `Unknown message type: ${data.type}`
    });
  });

  // =========================
  // CLOSE
  // =========================

  ws.on("close", () => {
    removeFromRoom(player, false);
    clients.delete(player.id);
  });

  ws.on("error", () => {
    removeFromRoom(player, false);
    clients.delete(player.id);
  });
});

// =========================
// CLEAN EMPTY ROOMS
// =========================

setInterval(() => {
  for (const [roomId, room] of rooms) {
    if (room.players.size === 0) {
      rooms.delete(roomId);
    }
  }
}, 30000);

// =========================
// START SERVER
// =========================

server.listen(PORT, "0.0.0.0", () => {
  console.log(
    `Clutcher Online server listening on 0.0.0.0:${PORT}`
  );
});