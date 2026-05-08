const express = require("express");
const http = require("http");
const crypto = require("crypto");
const cors = require("cors");
const { Server } = require("socket.io");
const { Chess } = require("chess.js");

const app = express();
const corsOrigin = process.env.CORS_ORIGIN || "*";

app.use(express.json({ limit: "200kb" }));
app.use(
  cors({
    origin: corsOrigin === "*" ? true : corsOrigin.split(",").map((s) => s.trim()),
  })
);

app.get("/", (_req, res) => res.type("text").send("Multiplayer server OK"));
app.get("/health", (_req, res) =>
  res.json({ ok: true, service: "multiplayer", ts: Date.now() })
);

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: corsOrigin === "*" ? true : corsOrigin.split(",").map((s) => s.trim()),
  },
  connectTimeout: 45_000,
  pingTimeout: 25_000,
  pingInterval: 10_000,
});

/** key -> [{ socket, uid, mode, betAmount }] */
const waitingQueues = new Map();
/** room -> state */
const roomStates = new Map();

const AFK_TIMEOUT_SEC = 120;
const AFK_TIMEOUT_MS = AFK_TIMEOUT_SEC * 1000;
const AFK_RESET_COOLDOWN_MS = 10_000;

function perPlayerMsForMode(_mode) {
  return 5 * 60 * 1000;
}

function queueKey(mode, betAmount) {
  return mode === "paid" ? `paid_${Number(betAmount) || 0}` : "free";
}

function oppositeColor(color) {
  return color === "white" ? "black" : "white";
}

function colorToTurn(color) {
  return color === "white" ? "w" : "b";
}

function isFinalized(st) {
  return !st || st.isFinished || st.gameOver;
}

function colorFromUid(st, uid) {
  if (!st || !uid) return null;
  if (uid === st.whiteUid) return "white";
  if (uid === st.blackUid) return "black";
  return null;
}

function createRoomState(room, gameId, mode, betAmount, whiteUid, blackUid) {
  const per = perPlayerMsForMode(mode);
  const now = Date.now();

  const st = {
    room,
    gameId,
    mode,
    betAmount,

    isFinished: false,
    gameOver: false,
    winner: null,

    chess: new Chess(),
    whiteUid,
    blackUid,

    whiteTime: per,
    blackTime: per,
    currentTurn: "white",
    turnClockStartedAt: now,

    // active socket ownership by uid
    socketByUid: {
      [whiteUid]: null,
      [blackUid]: null,
    },

    afk: {
      active: false,
      playerUid: null, // authoritative AFK owner
      deadlineAt: 0,
      lastBroadcastSec: -1,
      lastKnownDeadlineByUid: {},
      lastReturnAtByUid: {},
    },
  };

  roomStates.set(room, st);
  return st;
}

function applyElapsedToActiveClock(st, now = Date.now()) {
  const elapsed = Math.max(0, now - st.turnClockStartedAt);
  if (elapsed <= 0) return;

  if (st.currentTurn === "white") {
    st.whiteTime = Math.max(0, st.whiteTime - elapsed);
  } else {
    st.blackTime = Math.max(0, st.blackTime - elapsed);
  }

  st.turnClockStartedAt = now;
}

function liveClockPayloadFromState(st) {
  return {
    room: st.room,
    gameId: st.gameId,
    whiteRemainingMs: Math.max(0, st.whiteTime),
    blackRemainingMs: Math.max(0, st.blackTime),
    whiteTime: Math.max(0, st.whiteTime),
    blackTime: Math.max(0, st.blackTime),
    sideToMove: colorToTurn(st.currentTurn),
    currentTurn: st.currentTurn,
    turnClockStartedAt: st.turnClockStartedAt,
    serverNow: Date.now(),
  };
}

function emitClockEvents(st) {
  const live = liveClockPayloadFromState(st);

  io.to(st.room).emit("timerUpdate", {
    whiteTime: live.whiteTime,
    blackTime: live.blackTime,
  });

  io.to(st.room).emit("clockSync", live);
  io.to(st.room).emit("gameState", live);
}

function clearAfkState(st) {
  st.afk.active = false;
  st.afk.playerUid = null;
  st.afk.deadlineAt = 0;
  st.afk.lastBroadcastSec = -1;
}

function emitPlayerAfk(st, remainingSec) {
  if (!st.afk.active || !st.afk.playerUid) return;
  const c = colorFromUid(st, st.afk.playerUid);
  if (!c) return;

  io.to(st.room).emit("playerAFK", {
    player: c,
    remainingTime: Math.max(0, remainingSec),
  });
}

function destroyRoomState(room) {
  const st = roomStates.get(room);
  if (!st) return;
  clearAfkState(st);
  roomStates.delete(room);
}

function startAfkCountdown(room, playerUid) {
  const st = roomStates.get(room);
  if (isFinalized(st)) return;
  const playerColor = colorFromUid(st, playerUid);
  if (!playerColor) return;

  const now = Date.now();

  // same AFK owner -> keep existing deadline
  if (st.afk.active && st.afk.playerUid === playerUid) {
    const remainingSec = Math.ceil((st.afk.deadlineAt - now) / 1000);
    emitPlayerAfk(st, remainingSec);
    return;
  }

  // if AFK already owned by someone else, don't flip owner
  if (st.afk.active && st.afk.playerUid && st.afk.playerUid !== playerUid) {
    return;
  }

  let deadlineAt = now + AFK_TIMEOUT_MS;
  const oldDeadline = st.afk.lastKnownDeadlineByUid[playerUid] || 0;
  const lastReturnAt = st.afk.lastReturnAtByUid[playerUid] || 0;
  if (now - lastReturnAt < AFK_RESET_COOLDOWN_MS && oldDeadline > now) {
    deadlineAt = oldDeadline;
  }

  st.afk.active = true;
  st.afk.playerUid = playerUid;
  st.afk.deadlineAt = deadlineAt;
  st.afk.lastBroadcastSec = -1;
  st.afk.lastKnownDeadlineByUid[playerUid] = deadlineAt;

  const remainingSec = Math.ceil((deadlineAt - now) / 1000);
  emitPlayerAfk(st, remainingSec);

  console.log("[AFK] started", {
    room,
    uid: playerUid,
    color: playerColor,
    remainingSec,
    deadlineAt,
  });
}

function clearAfkCountdown(room, playerUid) {
  const st = roomStates.get(room);
  if (isFinalized(st)) return;
  if (!st.afk.active) return;
  if (st.afk.playerUid !== playerUid) return;

  const c = colorFromUid(st, playerUid);
  if (!c) return;

  st.afk.lastReturnAtByUid[playerUid] = Date.now();
  st.afk.lastKnownDeadlineByUid[playerUid] = 0;

  clearAfkState(st);

  io.to(room).emit("playerReturned", { player: c });
  console.log("[AFK] returned", { room, uid: playerUid, color: c });
}

function endMatch(room, payload) {
  const st = roomStates.get(room);
  if (isFinalized(st)) return;

  st.isFinished = true;
  st.gameOver = true;
  st.winner = payload.winnerColor ?? null;

  clearAfkState(st);

  io.to(room).emit("gameOver", {
    winner: st.winner,
    loser: payload.loserColor ?? null,
    reason: payload.gameOverReason || payload.reason || "Game ended",
  });

  io.to(room).emit("gameResult", {
    room,
    gameId: st.gameId,
    mode: st.mode,
    betAmount: st.betAmount,
    whiteUid: st.whiteUid,
    blackUid: st.blackUid,
    draw: !!payload.draw,
    winnerColor: payload.winnerColor ?? null,
    loserColor: payload.loserColor ?? null,
    reason: payload.reason,
    paid: st.mode === "paid" && st.betAmount > 0,
    balancesByUid: null,
  });

  destroyRoomState(room);
}

function handleChessTimeout(room, loserColor) {
  const winnerColor = oppositeColor(loserColor);
  const reasonText =
    loserColor === "white" ? "White ran out of time" : "Black ran out of time";

  endMatch(room, {
    reason: "timeout",
    gameOverReason: reasonText,
    winnerColor,
    loserColor,
    draw: false,
  });
}

function handleAfkAbandon(room, afkPlayerUid) {
  const st = roomStates.get(room);
  if (isFinalized(st)) return;

  const loserColor = colorFromUid(st, afkPlayerUid);
  if (!loserColor) return;
  const winnerColor = oppositeColor(loserColor);

  console.log("AFK PLAYER:", loserColor);
  console.log("WINNER SHOULD BE:", winnerColor);

  const reasonText =
    loserColor === "white" ? "White abandoned the game" : "Black abandoned the game";

  console.log("[AFK] timeout reached", {
    room,
    uid: afkPlayerUid,
    loser: loserColor,
    winner: winnerColor,
    reason: reasonText,
  });

  endMatch(room, {
    reason: "afk_abandon",
    gameOverReason: reasonText,
    winnerColor,
    loserColor,
    draw: false,
  });
}

function playerColorFromSocketMeta(meta) {
  if (!meta) return null;
  if (meta.color === "white") return "white";
  if (meta.color === "black") return "black";
  return null;
}

function tickActiveGames() {
  const now = Date.now();

  for (const [room, st] of roomStates.entries()) {
    if (isFinalized(st)) continue;

    // AFK resolution priority
    if (st.afk.active && st.afk.playerUid) {
      const remainingMs = st.afk.deadlineAt - now;
      const remainingSec = Math.max(0, Math.ceil(remainingMs / 1000));

      if (remainingSec !== st.afk.lastBroadcastSec) {
        st.afk.lastBroadcastSec = remainingSec;
        emitPlayerAfk(st, remainingSec);
        console.log("[AFK] countdown", {
          room,
          uid: st.afk.playerUid,
          remainingSec,
        });
      }

      if (remainingMs <= 0) {
        handleAfkAbandon(room, st.afk.playerUid);
        continue;
      }

      // keep clock sync for UI, but skip chess timeout checks while AFK active
      emitClockEvents(st);
      continue;
    }

    // normal chess clock
    applyElapsedToActiveClock(st, now);

    if (st.whiteTime <= 0) {
      handleChessTimeout(room, "white");
      continue;
    }
    if (st.blackTime <= 0) {
      handleChessTimeout(room, "black");
      continue;
    }

    emitClockEvents(st);
  }
}

io.on("connection", (socket) => {
  socket.on("joinGame", (payload = {}) => {
    const mode = payload.mode === "paid" ? "paid" : "free";
    const betAmount =
      mode === "paid" ? Math.max(0, Math.floor(Number(payload.betAmount) || 0)) : 0;
    const uid =
      typeof payload.uid === "string" && payload.uid.length > 0 ? payload.uid : socket.id;

    if (mode === "paid" && betAmount <= 0) {
      socket.emit("matchmaking_error", { message: "Invalid stake for paid match." });
      return;
    }

    const key = queueKey(mode, betAmount);
    if (!waitingQueues.has(key)) waitingQueues.set(key, []);
    const queue = waitingQueues.get(key);

    for (let i = queue.length - 1; i >= 0; i--) {
      const e = queue[i];
      if (!e?.socket?.connected) queue.splice(i, 1);
    }

    let first = null;
    while (queue.length > 0) {
      const candidate = queue.shift();
      if (candidate?.socket?.connected && candidate.socket.id !== socket.id) {
        first = candidate;
        break;
      }
    }

    if (!first) {
      queue.push({ socket, uid, mode, betAmount });
      socket.waitingQueueKey = key;
      socket.emit("waiting");
      return;
    }

    first.socket.waitingQueueKey = undefined;
    socket.waitingQueueKey = undefined;

    const room = `${first.socket.id}#${socket.id}`;
    const gameId = crypto.randomUUID();
    const whiteUid = first.uid;
    const blackUid = uid;

    first.socket.join(room);
    socket.join(room);

    first.socket.room = room;
    socket.room = room;

    first.socket.matchMeta = {
      room,
      color: "white",
      gameId,
      mode,
      betAmount,
      playerId: whiteUid, // uid
    };
    socket.matchMeta = {
      room,
      color: "black",
      gameId,
      mode,
      betAmount,
      playerId: blackUid, // uid
    };

    const st = createRoomState(room, gameId, mode, betAmount, whiteUid, blackUid);

    // set active socket ownership by uid
    st.socketByUid[whiteUid] = first.socket.id;
    st.socketByUid[blackUid] = socket.id;

    const startPayload = (color) => ({
      room,
      color,
      gameId,
      mode,
      betAmount: mode === "paid" ? betAmount : 0,
      whiteUid,
      blackUid,
      currentTurn: st.currentTurn,
      clock: liveClockPayloadFromState(st),
      gameOver: st.gameOver,
      winner: st.winner,
    });

    first.socket.emit("start", startPayload("white"));
    socket.emit("start", startPayload("black"));
    emitClockEvents(st);
  });

  socket.on("rejoinMatch", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : "";
    const uid =
      typeof payload.uid === "string" && payload.uid.length > 0 ? payload.uid : "";

    if (!room || !uid) {
      socket.emit("rejoinFailed", { message: "Invalid rejoin payload." });
      return;
    }

    const st = roomStates.get(room);
    if (isFinalized(st)) {
      socket.emit("rejoinFailed", { message: "Match not active." });
      return;
    }

    if (uid !== st.whiteUid && uid !== st.blackUid) {
      socket.emit("rejoinFailed", { message: "Not a player in this match." });
      return;
    }

    const color = uid === st.whiteUid ? "white" : "black";

    socket.join(room);
    socket.room = room;
    socket.matchMeta = {
      room,
      color,
      gameId: st.gameId,
      betAmount: st.betAmount,
      mode: st.mode,
      playerId: uid,
    };

    // transfer ownership to latest socket
    st.socketByUid[uid] = socket.id;

    // cancel AFK if same uid returned before timeout
    clearAfkCountdown(room, uid);

    socket.emit("rejoinOk", {
      room,
      color,
      gameId: st.gameId,
      fen: st.chess.fen(),
      moves: st.chess.history(),
      betAmount: st.betAmount,
      mode: st.mode,
      whiteUid: st.whiteUid,
      blackUid: st.blackUid,
      currentTurn: st.currentTurn,
      clock: liveClockPayloadFromState(st),
      gameOver: st.gameOver,
      winner: st.winner,
    });

    emitClockEvents(st);
  });

  socket.on("playerInactive", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const uid = socket.matchMeta?.playerId;
    if (!room || !uid) return;

    const st = roomStates.get(room);
    if (isFinalized(st)) return;
    if (st.socketByUid[uid] !== socket.id) return;

    startAfkCountdown(room, uid);
  });

  socket.on("playerActive", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const uid = socket.matchMeta?.playerId;
    if (!room || !uid) return;

    const st = roomStates.get(room);
    if (isFinalized(st)) return;
    if (st.socketByUid[uid] !== socket.id) return;

    clearAfkCountdown(room, uid);
  });

  socket.on("appBackgrounded", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const uid = socket.matchMeta?.playerId;
    if (!room || !uid) return;

    const st = roomStates.get(room);
    if (isFinalized(st)) return;
    if (st.socketByUid[uid] !== socket.id) return;

    startAfkCountdown(room, uid);
  });

  socket.on("appForegrounded", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const uid = socket.matchMeta?.playerId;
    if (!room || !uid) return;

    const st = roomStates.get(room);
    if (isFinalized(st)) return;
    if (st.socketByUid[uid] !== socket.id) return;

    clearAfkCountdown(room, uid);
  });

  socket.on("leaveGameScreen", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const uid = socket.matchMeta?.playerId;
    if (!room || !uid) return;

    const st = roomStates.get(room);
    if (isFinalized(st)) return;
    if (st.socketByUid[uid] !== socket.id) return;

    startAfkCountdown(room, uid);
  });

  socket.on("move", (data = {}) => {
    if (!data?.move) return;

    const room = typeof data.room === "string" ? data.room : "";
    if (!room || socket.room !== room || !socket.matchMeta) return;

    const st = roomStates.get(room);
    if (isFinalized(st)) return;

    // freeze moves while AFK countdown is active
    if (st.afk.active) return;

    const mover = playerColorFromSocketMeta(socket.matchMeta);
    if (!mover || mover !== st.currentTurn) return;

    const uid = socket.matchMeta.playerId;
    if (!uid || st.socketByUid[uid] !== socket.id) return;

    applyElapsedToActiveClock(st, Date.now());

    if (st.whiteTime <= 0) {
      handleChessTimeout(room, "white");
      return;
    }
    if (st.blackTime <= 0) {
      handleChessTimeout(room, "black");
      return;
    }

    const moveTry = st.chess.move({
      from: data.move.from,
      to: data.move.to,
      promotion: data.move.promotion || "q",
    });
    if (!moveTry) return;

    st.currentTurn = st.currentTurn === "white" ? "black" : "white";
    st.turnClockStartedAt = Date.now();

    io.to(room).emit("move", data.move);
    emitClockEvents(st);

    if (st.chess.isCheckmate()) {
      const loserTurn = st.chess.turn();
      const loserColor = loserTurn === "w" ? "white" : "black";
      const winnerColor = oppositeColor(loserColor);

      endMatch(room, {
        reason: "checkmate",
        gameOverReason:
          winnerColor === "white" ? "Checkmate - White wins" : "Checkmate - Black wins",
        winnerColor,
        loserColor,
        draw: false,
      });
      return;
    }

    if (st.chess.isDraw()) {
      endMatch(room, {
        reason: "draw",
        gameOverReason: "Draw",
        winnerColor: null,
        loserColor: null,
        draw: true,
      });
    }
  });

  socket.on("playerResigned", () => {
    const meta = socket.matchMeta;
    if (!meta?.room || !meta?.color || !meta?.playerId) return;

    const st = roomStates.get(meta.room);
    if (isFinalized(st)) return;
    if (st.socketByUid[meta.playerId] !== socket.id) return;

    const loserColor = meta.color;
    const winnerColor = oppositeColor(loserColor);

    endMatch(meta.room, {
      reason: "playerResigned",
      gameOverReason:
        winnerColor === "white"
          ? "Black resigned - White wins"
          : "White resigned - Black wins",
      winnerColor,
      loserColor,
      draw: false,
    });
  });

  socket.on("resign", () => {
    const meta = socket.matchMeta;
    if (!meta?.room || !meta?.color || !meta?.playerId) return;

    const st = roomStates.get(meta.room);
    if (isFinalized(st)) return;
    if (st.socketByUid[meta.playerId] !== socket.id) return;

    const loserColor = meta.color;
    const winnerColor = oppositeColor(loserColor);

    endMatch(meta.room, {
      reason: "resign",
      gameOverReason:
        winnerColor === "white"
          ? "Black resigned - White wins"
          : "White resigned - Black wins",
      winnerColor,
      loserColor,
      draw: false,
    });
  });

  socket.on("disconnect", () => {
    // remove from matchmaking queue
    const key = socket.waitingQueueKey;
    if (key && waitingQueues.has(key)) {
      const q = waitingQueues.get(key);
      const idx = q.findIndex((e) => e.socket === socket);
      if (idx >= 0) q.splice(idx, 1);
      if (q.length === 0) waitingQueues.delete(key);
    }

    // AFK start only if disconnected socket is current owner for that uid
    const meta = socket.matchMeta;
    const room = meta?.room;
    const uid = meta?.playerId;
    if (!room || !uid) return;

    const st = roomStates.get(room);
    if (isFinalized(st)) return;

    if (st.socketByUid[uid] !== socket.id) return; // stale disconnect ignored
    st.socketByUid[uid] = null;

    startAfkCountdown(room, uid);
  });
});

let activeTicker = null;
function startTicker() {
  if (activeTicker) return;
  activeTicker = setInterval(tickActiveGames, 1000);
}
startTicker();

const PORT = Number(process.env.PORT) || 4000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`Multiplayer server listening on ${PORT}`);
});
```

