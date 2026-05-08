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
const AFK_RESET_COOLDOWN_MS = 10_000; // anti reconnect-spam reset
function perPlayerMsForMode(_mode) {
  // forced 5 min both modes
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
function turnToColor(turn) {
  return turn === "w" ? "white" : "black";
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
    chess: new Chess(),
    whiteUid,
    blackUid,
    // Required fields
    whiteTime: per,
    blackTime: per,
    currentTurn: "white",
    winner: null,
    gameOver: false,
    // Clock state
    turnClockStartedAt: now,
    // AFK state
    afk: {
      active: false,
      player: null, // "white" | "black" | null
      deadlineAt: 0,
      lastBroadcastSec: -1,
      lastKnownDeadlineByPlayer: {
        white: 0,
        black: 0,
      },
      lastReturnAtByPlayer: {
        white: 0,
        black: 0,
      },
    },
  };
  roomStates.set(room, st);
  return st;
}
/**
 * Applies elapsed real time to the side-to-move clock.
 * Keeps whiteTime/blackTime as authoritative bank values.
 */
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
  // Required timer event
  io.to(st.room).emit("timerUpdate", {
    whiteTime: live.whiteTime,
    blackTime: live.blackTime,
  });
  // Existing client compatibility
  io.to(st.room).emit("clockSync", live);
  io.to(st.room).emit("gameState", live);
}
function clearAfkState(st) {
  st.afk.active = false;
  st.afk.player = null;
  st.afk.deadlineAt = 0;
  st.afk.lastBroadcastSec = -1;
}
function destroyRoomState(room) {
  const st = roomStates.get(room);
  if (!st) return;
  clearAfkState(st);
  roomStates.delete(room);
}
function emitPlayerAfk(st, remainingSec) {
  if (!st.afk.active || !st.afk.player) return;
  io.to(st.room).emit("playerAFK", {
    player: st.afk.player,
    remainingTime: Math.max(0, remainingSec),
  });
}
function startAfkCountdown(room, playerColor) {
  const st = roomStates.get(room);
  if (!st || st.isFinished || st.gameOver) return;
  if (playerColor !== "white" && playerColor !== "black") return;
  const now = Date.now();
  // Already AFK by same player: keep existing deadline (no reset)
  if (st.afk.active && st.afk.player === playerColor) {
    const remainingSec = Math.ceil((st.afk.deadlineAt - now) / 1000);
    emitPlayerAfk(st, remainingSec);
    return;
  }
  let deadlineAt = now + AFK_TIMEOUT_MS;
  // Anti-cheat: if player reconnect-spams quickly, don't grant full reset
  const oldDeadline = st.afk.lastKnownDeadlineByPlayer[playerColor] || 0;
  const lastReturnAt = st.afk.lastReturnAtByPlayer[playerColor] || 0;
  if (now - lastReturnAt < AFK_RESET_COOLDOWN_MS && oldDeadline > now) {
    deadlineAt = oldDeadline;
  }
  st.afk.active = true;
  st.afk.player = playerColor;
  st.afk.deadlineAt = deadlineAt;
  st.afk.lastBroadcastSec = -1;
  st.afk.lastKnownDeadlineByPlayer[playerColor] = deadlineAt;
  const remainingSec = Math.ceil((deadlineAt - now) / 1000);
  emitPlayerAfk(st, remainingSec);
}
function clearAfkCountdown(room, playerColor) {
  const st = roomStates.get(room);
  if (!st || st.isFinished || st.gameOver) return;
  if (!st.afk.active) return;
  if (st.afk.player !== playerColor) return;
  st.afk.lastReturnAtByPlayer[playerColor] = Date.now();
  clearAfkState(st);
  io.to(room).emit("playerReturned", { player: playerColor });
}
function endMatch(room, payload) {
  const st = roomStates.get(room);
  if (!st || st.isFinished || st.gameOver) return;
  st.isFinished = true;
  st.gameOver = true;
  st.winner = payload.winnerColor ?? null;
  // Required
  io.to(room).emit("gameOver", {
    winner: st.winner,
    reason: payload.gameOverReason || payload.reason || "Game ended",
  });
  // Existing compatibility
  io.to(room).emit("gameResult", {
    room,
    gameId: st.gameId,
    mode: st.mode,
    betAmount: st.betAmount,
    whiteUid: st.whiteUid,
    blackUid: st.blackUid,
    draw: !!payload.draw,
    winnerColor: payload.winnerColor ?? null,
    reason: payload.reason,
    paid: st.mode === "paid" && st.betAmount > 0,
    balancesByUid: null,
  });
  destroyRoomState(room);
}
function handleChessTimeout(room, loserColor) {
  const winnerColor = oppositeColor(loserColor);
  const reasonText = loserColor === "white" ? "White ran out of time" : "Black ran out of time";
  endMatch(room, {
    reason: "timeout",
    gameOverReason: reasonText,
    winnerColor,
    draw: false,
  });
}
function handleAfkAbandon(room, afkPlayerColor) {
  const winnerColor = oppositeColor(afkPlayerColor);
  const reasonText =
    afkPlayerColor === "white" ? "White abandoned the game" : "Black abandoned the game";
  endMatch(room, {
    reason: "afk_abandon",
    gameOverReason: reasonText,
    winnerColor,
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
    if (!st || st.isFinished || st.gameOver) continue;
    // 1) Chess clock (authoritative backend)
    applyElapsedToActiveClock(st, now);
    if (st.whiteTime <= 0) {
      handleChessTimeout(room, "white");
      continue;
    }
    if (st.blackTime <= 0) {
      handleChessTimeout(room, "black");
      continue;
    }
    // 2) AFK countdown (authoritative backend)
    if (st.afk.active && st.afk.player) {
      const remainingMs = st.afk.deadlineAt - now;
      const remainingSec = Math.max(0, Math.ceil(remainingMs / 1000));
      if (remainingSec !== st.afk.lastBroadcastSec) {
        st.afk.lastBroadcastSec = remainingSec;
        emitPlayerAfk(st, remainingSec);
      }
      if (remainingMs <= 0) {
        handleAfkAbandon(room, st.afk.player);
        continue;
      }
    }
    // 3) Broadcast live clocks each second
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
// remove stale entries first
for (let i = queue.length - 1; i >= 0; i--) {
  const e = queue[i];
  if (!e?.socket?.connected) queue.splice(i, 1);
}
// find first valid opponent (not same socket)
let first = null;
while (queue.length > 0) {
  const candidate = queue.shift();
  if (candidate?.socket?.connected && candidate.socket.id !== socket.id) {
    first = candidate;
    break;
  }
}
// if no opponent, enqueue current player
if (!first) {
  queue.push({ socket, uid, mode, betAmount });
  socket.waitingQueueKey = key;
  socket.emit("waiting");
  return;
}
// matched: clear waiting keys
first.socket.waitingQueueKey = undefined;
socket.waitingQueueKey = undefined;
// ... then create room and emit "start" as you already do

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
      playerId: whiteUid,
    };
    socket.matchMeta = {
      room,
      color: "black",
      gameId,
      mode,
      betAmount,
      playerId: blackUid,
    };
    const st = createRoomState(room, gameId, mode, betAmount, whiteUid, blackUid);
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
    if (!st || st.isFinished || st.gameOver) {
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
    // Player returned before AFK timeout
    clearAfkCountdown(room, color);
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
  // Optional explicit inactivity hooks from frontend
  socket.on("playerInactive", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const player =
      payload.player === "white" || payload.player === "black"
        ? payload.player
        : socket.matchMeta?.color;
    if (!room || (player !== "white" && player !== "black")) return;
    startAfkCountdown(room, player);
  });
  socket.on("playerActive", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const player =
      payload.player === "white" || payload.player === "black"
        ? payload.player
        : socket.matchMeta?.color;
    if (!room || (player !== "white" && player !== "black")) return;
    clearAfkCountdown(room, player);
  });
  // Aliases for app lifecycle / screen-leave
  socket.on("appBackgrounded", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const player = socket.matchMeta?.color;
    if (!room || (player !== "white" && player !== "black")) return;
    startAfkCountdown(room, player);
  });
  socket.on("appForegrounded", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const player = socket.matchMeta?.color;
    if (!room || (player !== "white" && player !== "black")) return;
    clearAfkCountdown(room, player);
  });
  socket.on("leaveGameScreen", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : socket.matchMeta?.room;
    const player = socket.matchMeta?.color;
    if (!room || (player !== "white" && player !== "black")) return;
    startAfkCountdown(room, player);
  });
  socket.on("move", (data = {}) => {
    if (!data?.move) return;
    const room = typeof data.room === "string" ? data.room : "";
    if (!room || socket.room !== room || !socket.matchMeta) return;
    const st = roomStates.get(room);
    if (!st || st.isFinished || st.gameOver) return;
    const mover = playerColorFromSocketMeta(socket.matchMeta);
    if (!mover || mover !== st.currentTurn) return;
    // Apply elapsed time before processing move
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
    // Switch turn and reset turn anchor
    st.currentTurn = st.currentTurn === "white" ? "black" : "white";
    st.turnClockStartedAt = Date.now();
    io.to(room).emit("move", data.move);
    emitClockEvents(st);
    if (st.chess.isCheckmate()) {
      const loserTurn = st.chess.turn(); // side to move after mate
      const winnerColor = loserTurn === "w" ? "black" : "white";
      endMatch(room, {
        reason: "checkmate",
        gameOverReason:
          winnerColor === "white" ? "Checkmate - White wins" : "Checkmate - Black wins",
        winnerColor,
        draw: false,
      });
      return;
    }
    if (st.chess.isDraw()) {
      endMatch(room, {
        reason: "draw",
        gameOverReason: "Draw",
        winnerColor: null,
        draw: true,
      });
    }
  });
  socket.on("playerResigned", () => {
    const meta = socket.matchMeta;
    if (!meta?.room || !meta?.color) return;
    const winnerColor = meta.color === "white" ? "black" : "white";
    endMatch(meta.room, {
      reason: "playerResigned",
      gameOverReason:
        winnerColor === "white"
          ? "Black resigned - White wins"
          : "White resigned - Black wins",
      winnerColor,
      draw: false,
    });
  });
  socket.on("resign", () => {
    const meta = socket.matchMeta;
    if (!meta?.room || !meta?.color) return;
    const winnerColor = meta.color === "white" ? "black" : "white";
    endMatch(meta.room, {
      reason: "resign",
      gameOverReason:
        winnerColor === "white"
          ? "Black resigned - White wins"
          : "White resigned - Black wins",
      winnerColor,
      draw: false,
    });
  });
  socket.on("disconnect", () => {
    // Remove from matchmaking queue
    const key = socket.waitingQueueKey;
    if (key && waitingQueues.has(key)) {
      const q = waitingQueues.get(key);
      const idx = q.findIndex((e) => e.socket === socket);
      if (idx >= 0) q.splice(idx, 1);
      if (q.length === 0) waitingQueues.delete(key);
    }
    // Mark AFK if player was in active match
    const meta = socket.matchMeta;
    if (meta?.room && (meta.color === "white" || meta.color === "black")) {
      const st = roomStates.get(meta.room);
      if (st && !st.isFinished && !st.gameOver) {
        startAfkCountdown(meta.room, meta.color);
      }
    }
  });
});
/** Single global interval: no duplicate per-room intervals */
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
