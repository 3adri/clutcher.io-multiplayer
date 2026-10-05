const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = Number(process.env.PORT || 10000);

const MAX_PLAYERS = 32;
const MAX_ROOMS = 100;
const MAX_NAME = 24;
const MAX_CHAT = 300;

const TICK = 50;
const ROUND_TIME = 120000;
const ROUND_TRANSITION = 3500;

const rooms = new Map();
const clients = new Map();

function id(prefix) {
  return `${prefix}_${crypto.randomBytes(6).toString("hex")}`;
}

function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function clamp(v, a, b) {
  return Math.max(a, Math.min(b, v));
}

function name(v) {
  return String(v || "Player")
    .replace(/[^\p{L}\p{N}_ .-]/gu, "")
    .trim()
    .slice(0, MAX_NAME) || "Player";
}

function roomName(v) {
  return String(v || "Nueva partida")
    .replace(/[\u0000-\u001f\u007f]/g, "")
    .trim()
    .slice(0, 50) || "Nueva partida";
}

function send(ws, data) {
  if (!ws || ws.readyState !== 1) return false;

  try {
    ws.send(JSON.stringify(data));
    return true;
  } catch {
    return false;
  }
}

function broadcast(room, data, except = null) {
  for (const p of room.players.values()) {
    if (p.id !== except) {
      send(p.ws, data);
    }
  }
}

function roomInfo(r) {
  return {
    id: r.id,
    name: r.name,
    players: r.players.size,
    maxPlayers: r.maxPlayers,
    teamSize: r.teamSize,

    round: r.round,
    maxRounds: r.maxRounds,

    timeLeft: r.roundActive
      ? Math.max(0, r.roundEndsAt - Date.now())
      : 0,

    roundActive: r.roundActive,

    score: r.score,

    transitionLeft: r.transitionUntil
      ? Math.max(0, r.transitionUntil - Date.now())
      : 0
  };
}

function pub(p) {
  return {
    id: p.id,
    name: p.name,
    team: p.team,

    state: p.state,

    kills: p.stats.kills,
    deaths: p.stats.deaths,
    assists: p.stats.assists,

    ...p.state
  };
}

function snapshot(r) {
  return {
    type: "room_snapshot",
    room: roomInfo(r),
    players: [...r.players.values()].map(pub)
  };
}

function roomsList() {
  return [...rooms.values()]
    .map(roomInfo)
    .sort((a, b) => a.name.localeCompare(b.name));
}

function sendRooms(ws) {
  send(ws, {
    type: "rooms",
    rooms: roomsList()
  });
}

function allRooms() {
  for (const p of clients.values()) {
    sendRooms(p.ws);
  }
}

function err(ws, m) {
  send(ws, {
    type: "error",
    message: String(m).slice(0, 300)
  });
}

function newState() {
  return {
    x: 0,
    y: 0,
    z: 0,

    vx: 0,
    vy: 0,
    vz: 0,

    yaw: 0,
    pitch: 0,

    onGround: true,

    alive: true,

    crouch: false,
    crouching: false,
    walking: false,

    health: 100,
    armor: 0,

    weapon: null,
    ammo: 0,
    reserve: 0
  };
}

/*
 * EQUIPOS
 *
 * 0 = CT
 * 1 = T
 *
 * Siempre intenta mantener los equipos equilibrados.
 */
function assignTeam(room) {
  let ct = 0;
  let t = 0;

  for (const p of room.players.values()) {
    if (p.team === 0) ct++;
    else t++;
  }

  return ct <= t ? 0 : 1;
}

function publicMatch(room) {
  return {
    type: "match_state",

    room: roomInfo(room),

    players: [...room.players.values()].map(pub)
  };
}

function sendMatch(room) {
  broadcast(room, publicMatch(room));
}

function living(room, team) {
  return [...room.players.values()]
    .filter(
      p =>
        p.team === team &&
        p.state.alive
    ).length;
}

function hasTeam(room, team) {
  return [...room.players.values()]
    .some(p => p.team === team);
}

/*
 * INICIO DE RONDA
 */
function startRound(room) {
  room.roundActive = true;

  room.transitionUntil = 0;

  room.roundStartedAt = Date.now();

  room.roundEndsAt =
    Date.now() + ROUND_TIME;

  /*
   * Todos vuelven vivos al comenzar
   * la nueva ronda.
   */
  for (const p of room.players.values()) {
    p.state.alive = true;

    p.state.health = 100;
    p.state.armor = 0;

    p.state.vx = 0;
    p.state.vy = 0;
    p.state.vz = 0;

    p.stats.roundKills = 0;
  }

  broadcast(room, {
    type: "round_start",

    round: room.round,
    maxRounds: room.maxRounds,

    timeLeft: ROUND_TIME,

    score: room.score,

    countdown: 0
  });

  sendMatch(room);
}

/*
 * FIN DE RONDA
 */
function endRound(
  room,
  winnerTeam = null,
  reason = "elimination"
) {
  if (!room.roundActive) return;

  room.roundActive = false;

  room.transitionUntil =
    Date.now() + ROUND_TRANSITION;

  room.roundEndsAt = 0;

  /*
   * Sumar punto al equipo ganador.
   */
  if (
    winnerTeam === 0 ||
    winnerTeam === 1
  ) {
    room.score[winnerTeam]++;
  }

  /*
   * Todos muertos durante la transición.
   */
  for (const p of room.players.values()) {
    p.state.alive = false;
    p.state.health = 0;

    p.state.vx = 0;
    p.state.vy = 0;
    p.state.vz = 0;
  }

  broadcast(room, {
    type: "round_end",

    round: room.round,
    maxRounds: room.maxRounds,

    score: room.score,

    winnerTeam,

    reason,

    countdown: ROUND_TRANSITION,

    nextRound: room.round + 1
  });

  sendMatch(room);

  /*
   * Esperar la cuenta atrás.
   */
  setTimeout(() => {
    if (!rooms.has(room.id)) return;
    if (room.players.size === 0) return;

    /*
     * Si se han terminado todas las rondas,
     * empezamos una partida nueva.
     */
    if (room.round >= room.maxRounds) {
      room.round = 1;

      room.score = {
        0: 0,
        1: 0
      };
    } else {
      room.round++;
    }

    startRound(room);
  }, ROUND_TRANSITION);
}

/*
 * Comprueba si un equipo se ha quedado
 * sin jugadores vivos.
 */
function checkRound(
  room,
  preferredWinner = null
) {
  if (!room.roundActive) return;

  const ctExists = hasTeam(room, 0);
  const tExists = hasTeam(room, 1);

  if (!ctExists || !tExists) {
    return;
  }

  const ctAlive = living(room, 0);
  const tAlive = living(room, 1);

  /*
   * Los dos equipos muertos.
   */
  if (
    ctAlive === 0 &&
    tAlive === 0
  ) {
    endRound(
      room,
      preferredWinner,
      "elimination"
    );

    return;
  }

  /*
   * CT eliminado.
   */
  if (ctAlive === 0) {
    endRound(
      room,
      1,
      "elimination"
    );

    return;
  }

  /*
   * T eliminado.
   */
  if (tAlive === 0) {
    endRound(
      room,
      0,
      "elimination"
    );

    return;
  }
}

/*
 * ELIMINAR JUGADOR DE LA SALA
 */
function remove(
  p,
  announce = true
) {
  if (!p.roomId) return;

  const r = rooms.get(p.roomId);

  const rid = p.roomId;

  p.roomId = null;

  if (!r) return;

  r.players.delete(p.id);

  if (announce) {
    broadcast(
      r,
      {
        type: "player_left",

        id: p.id,
        playerId: p.id
      },
      p.id
    );
  }

  send(p.ws, {
    type: "left_room",
    roomId: rid
  });

  if (r.players.size === 0) {
    rooms.delete(r.id);
  } else {
    sendMatch(r);
    checkRound(r);
  }

  allRooms();
}

/*
 * UNIR JUGADOR
 */
function join(p, r) {
  if (!r) {
    err(
      p.ws,
      "La partida no existe."
    );

    return;
  }

  if (
    r.players.size >=
    r.maxPlayers
  ) {
    err(
      p.ws,
      "La partida está llena."
    );

    return;
  }

  if (p.roomId) {
    remove(p, true);
  }

  p.roomId = r.id;

  /*
   * Asignación automática equilibrada.
   */
  p.team = assignTeam(r);

  r.players.set(
    p.id,
    p
  );

  const players =
    [...r.players.values()]
      .map(pub);

  send(p.ws, {
    type: "room_joined",

    roomId: r.id,

    room: roomInfo(r),

    players
  });

  broadcast(
    r,
    {
      type: "player_joined",

      player: pub(p)
    },
    p.id
  );

  sendMatch(r);

  if (!r.roundActive) {
    startRound(r);
  }

  allRooms();
}

/*
 * CREAR SALA
 */
function create(p, d) {
  if (
    rooms.size >= MAX_ROOMS
  ) {
    err(
      p.ws,
      "Máximo de partidas alcanzado."
    );

    return;
  }

  const max =
    clamp(
      num(
        d.maxPlayers ??
        d.max_players,
        16
      ),
      2,
      MAX_PLAYERS
    );

  const teamSize =
    clamp(
      num(
        d.teamSize ??
        d.team_size,
        8
      ),
      1,
      16
    );

  const r = {
    id: id("room"),

    name:
      roomName(
        d.name ||
        d.room
      ),

    maxPlayers: max,

    teamSize,

    players: new Map(),

    createdAt:
      Date.now(),

    round: 1,

    maxRounds: 16,

    roundActive: false,

    roundStartedAt: 0,

    roundEndsAt: 0,

    transitionUntil: 0,

    score: {
      0: 0,
      1: 0
    }
  };

  rooms.set(
    r.id,
    r
  );

  join(
    p,
    r
  );
}

/*
 * ACTUALIZAR ESTADO
 */
function state(p, s) {
  s =
    s &&
    typeof s === "object"
      ? s
      : {};

  const q = p.state;

  q.x =
    clamp(
      num(s.x),
      -100000,
      100000
    );

  q.y =
    clamp(
      num(s.y),
      -100000,
      100000
    );

  q.z =
    clamp(
      num(s.z),
      -100000,
      100000
    );

  q.vx =
    clamp(
      num(s.vx),
      -1000,
      1000
    );

  q.vy =
    clamp(
      num(s.vy),
      -1000,
      1000
    );

  q.vz =
    clamp(
      num(s.vz),
      -1000,
      1000
    );

  q.yaw =
    num(s.yaw);

  q.pitch =
    num(s.pitch);

  q.onGround =
    !!s.onGround;

  q.alive =
    s.alive !== false;

  q.crouch =
    !!(
      s.crouch ??
      s.crouching
    );

  q.crouching =
    q.crouch;

  q.walking =
    !!s.walking;

  q.health =
    clamp(
      num(
        s.health,
        100
      ),
      0,
      100
    );

  q.armor =
    clamp(
      num(
        s.armor,
        0
      ),
      0,
      100
    );

  q.weapon =
    String(
      s.weapon || ""
    ).slice(
      0,
      64
    );

  q.ammo =
    clamp(
      num(s.ammo),
      0,
      999
    );

  q.reserve =
    clamp(
      num(s.reserve),
      0,
      999
    );
}

/*
 * BUSCAR SALA
 */
function findRoom(d) {
  const key =
    String(
      d?.roomId ??
      d?.room ??
      d?.id ??
      ""
    ).trim();

  if (!key) {
    return null;
  }

  if (rooms.has(key)) {
    return rooms.get(key);
  }

  for (const r of rooms.values()) {
    if (r.name === key) {
      return r;
    }
  }

  return null;
}

/*
 * PROCESAR MENSAJES
 */
function handle(p, d) {
  if (
    !d ||
    typeof d !== "object"
  ) {
    return;
  }

  switch (d.type) {

    /*
     * HELLO
     */
    case "hello":

      if (d.name) {
        p.name =
          name(d.name);
      }

      send(
        p.ws,
        {
          type: "hello",

          id: p.id,

          playerId: p.id,

          version:
            "7.0.0"
        }
      );

      sendRooms(
        p.ws
      );

      break;


    /*
     * LISTAR SALAS
     */
    case "list_rooms":

      sendRooms(
        p.ws
      );

      break;


    /*
     * CREAR SALA
     */
    case "create_room":

      create(
        p,
        d
      );

      break;


    /*
     * JOIN
     */
    case "join_room": {

      const r =
        findRoom(d);

      if (!r) {
        return err(
          p.ws,
          "La partida no existe."
        );
      }

      join(
        p,
        r
      );

      break;
    }


    /*
     * LEAVE
     */
    case "leave_room":

      remove(
        p,
        true
      );

      break;


    /*
     * CAMBIAR NOMBRE
     */
    case "set_name":

      p.name =
        name(
          d.name
        );

      if (p.roomId) {

        const r =
          rooms.get(
            p.roomId
          );

        if (r) {

          broadcast(
            r,
            {
              type:
                "player_state",

              player:
                pub(p)
            },
            p.id
          );
        }
      }

      break;


    /*
     * ESTADO DEL JUGADOR
     */
    case "player_state":

      if (p.roomId) {

        const r =
          rooms.get(
            p.roomId
          );

        if (r) {

          state(
            p,
            d.state ||
            d
          );

          broadcast(
            r,
            {
              type:
                "player_state",

              player:
                pub(p)
            },
            p.id
          );
        }
      }

      break;


    /*
     * DISPARO
     */
    case "fire":
    case "shot":

      if (p.roomId) {

        const r =
          rooms.get(
            p.roomId
          );

        if (r) {

          broadcast(
            r,
            {
              type:
                "fire",

              id:
                p.id,

              playerId:
                p.id,

              player:
                pub(p),

              yaw:
                num(
                  d.yaw ??
                  d.shot?.yaw,
                  p.state.yaw
                ),

              pitch:
                num(
                  d.pitch ??
                  d.shot?.pitch,
                  p.state.pitch
                ),

              weapon:
                d.weapon ||
                d.shot?.weapon ||
                p.state.weapon,

              ammo:
                num(
                  d.ammo ??
                  d.shot?.ammo,
                  p.state.ammo
                )
            },
            p.id
          );
        }
      }

      break;


    /*
     * DAÑO
     */
    case "damage": {

      if (!p.roomId) {
        break;
      }

      const r =
        rooms.get(
          p.roomId
        );

      if (
        !r ||
        !r.roundActive ||
        !p.state.alive
      ) {
        break;
      }

      const target =
        r.players.get(
          String(
            d.targetId ??
            d.victim ??
            d.target
          )
        );

      if (
        !target ||
        !target.state.alive ||
        target.team === p.team
      ) {
        break;
      }

      const amount =
        clamp(
          num(
            d.amount ??
            d.damage
          ),
          0,
          100
        );

      target.state.health =
        Math.max(
          0,
          target.state.health -
            amount
        );

      /*
       * MUERTE
       */
      if (
        target.state.health <= 0
      ) {

        target.state.alive =
          false;

        target.stats.deaths++;

        p.stats.kills++;

        p.stats.roundKills =
          (p.stats.roundKills || 0) +
          1;
      }

      /*
       * Avisar daño.
       */
      broadcast(
        r,
        {
          type:
            "damage",

          attackerId:
            p.id,

          attacker:
            p.id,

          targetId:
            target.id,

          victim:
            target.id,

          amount,

          damage:
            amount,

          headshot:
            !!d.headshot,

          weapon:
            String(
              d.weapon || ""
            ).slice(
              0,
              64
            ),

          health:
            target.state.health
        },
        null
      );

      /*
       * Si ha muerto.
       */
      if (
        target.state.health <= 0
      ) {

        broadcast(
          r,
          {
            type:
              "kill",

            attackerId:
              p.id,

            killerId:
              p.id,

            targetId:
              target.id,

            victim:
              target.id,

            weapon:
              String(
                d.weapon || ""
              ).slice(
                0,
                64
              ),

            headshot:
              !!d.headshot
          },
          null
        );

        sendMatch(r);

        checkRound(
          r,
          p.team
        );

      } else {

        sendMatch(r);
      }

      break;
    }


    /*
     * KILL DIRECTO
     */
    case "kill": {

      if (!p.roomId) {
        break;
      }

      const r =
        rooms.get(
          p.roomId
        );

      if (
        !r ||
        !r.roundActive
      ) {
        break;
      }

      const t =
        r.players.get(
          String(
            d.targetId ??
            d.victim ??
            d.target
          )
        );

      if (
        !t ||
        !t.state.alive ||
        t.team === p.team
      ) {
        break;
      }

      t.state.alive =
        false;

      t.state.health =
        0;

      t.stats.deaths++;

      p.stats.kills++;

      p.stats.roundKills =
        (p.stats.roundKills || 0) +
        1;

      broadcast(
        r,
        {
          type:
            "kill",

          attackerId:
            p.id,

          killerId:
            p.id,

          targetId:
            t.id,

          victim:
            t.id,

          weapon:
            String(
              d.weapon || ""
            ).slice(
              0,
              64
            ),

          headshot:
            !!d.headshot
        },
        null
      );

      sendMatch(r);

      checkRound(
        r,
        p.team
      );

      break;
    }


    /*
     * RESPawn
     *
     * El servidor controla el respawn
     * durante el modo de rondas.
     */
    case "respawn":

      break;


    /*
     * CHAT
     */
    case "chat":

      if (p.roomId) {

        const r =
          rooms.get(
            p.roomId
          );

        if (r) {

          const m =
            String(
              d.message ??
              d.text ??
              ""
            )
              .replace(
                /[\u0000-\u001f\u007f]/g,
                ""
              )
              .trim()
              .slice(
                0,
                MAX_CHAT
              );

          if (m) {

            broadcast(
              r,
              {
                type:
                  "chat",

                id:
                  p.id,

                name:
                  p.name,

                message:
                  m,

                text:
                  m
              }
            );
          }
        }
      }

      break;


    /*
     * PING
     */
    case "ping":

      send(
        p.ws,
        {
          type:
            "pong",

          t:
            d.t ??
            Date.now()
        }
      );

      break;
  }
}


/*
 * HTTP
 */
app.get("/", (_, res) => {
  res.json({
    ok: true,

    service:
      "clutcher-io-online",

    websocket:
      true,

    rooms:
      rooms.size,

    clients:
      clients.size
  });
});

app.get("/health", (_, res) => {
  res.json({
    ok: true,

    rooms:
      rooms.size,

    clients:
      clients.size
  });
});


/*
 * WEBSOCKET
 */
const wss =
  new WebSocketServer({
    server,

    maxPayload:
      64 * 1024
  });


wss.on(
  "connection",
  ws => {

    const p = {

      id:
        id("player"),

      name:
        "Player",

      ws,

      roomId:
        null,

      team:
        0,

      state:
        newState(),

      stats: {
        kills: 0,
        deaths: 0,
        assists: 0,
        roundKills: 0
      },

      connectedAt:
        Date.now()
    };


    clients.set(
      p.id,
      p
    );


    send(
      ws,
      {
        type:
          "connected",

        id:
          p.id,

        playerId:
          p.id
      }
    );


    ws.on(
      "message",
      raw => {

        try {

          handle(
            p,
            JSON.parse(
              raw.toString()
            )
          );

        } catch {

          err(
            ws,
            "JSON inválido"
          );
        }
      }
    );


    ws.on(
      "close",
      () => {

        remove(
          p,
          true
        );

        clients.delete(
          p.id
        );

        allRooms();
      }
    );


    ws.on(
      "error",
      () => {}
    );
  }
);


/*
 * RELOJ GLOBAL DE LAS PARTIDAS
 *
 * Se manda cada 50 ms para que todos
 * los clientes tengan el mismo tiempo.
 */
setInterval(
  () => {

    for (const r of rooms.values()) {

      if (
        r.players.size === 0
      ) {
        rooms.delete(
          r.id
        );

        continue;
      }


      /*
       * Tiempo agotado.
       */
      if (
        r.roundActive &&
        Date.now() >=
          r.roundEndsAt
      ) {

        const ct =
          living(r, 0);

        const t =
          living(r, 1);

        let winner = null;

        if (ct > t) {
          winner = 0;
        } else if (t > ct) {
          winner = 1;
        }

        endRound(
          r,
          winner,
          "timeout"
        );
      }


      /*
       * Ronda activa.
       */
      if (r.roundActive) {

        broadcast(
          r,
          {
            type:
              "clock",

            round:
              r.round,

            maxRounds:
              r.maxRounds,

            timeLeft:
              Math.max(
                0,
                r.roundEndsAt -
                  Date.now()
              ),

            roundActive:
              true,

            score:
              r.score
          },
          null
        );

      }


      /*
       * Cuenta atrás entre rondas.
       */
      else if (
        r.transitionUntil
      ) {

        broadcast(
          r,
          {
            type:
              "clock",

            round:
              r.round,

            maxRounds:
              r.maxRounds,

            timeLeft:
              0,

            roundActive:
              false,

            transitionLeft:
              Math.max(
                0,
                r.transitionUntil -
                  Date.now()
              ),

            score:
              r.score
          },
          null
        );
      }
    }

  },
  TICK
);


/*
 * INICIAR SERVIDOR
 */
server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `Clutcher Online server listening on 0.0.0.0:${PORT}`
    );
  }
);
