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
app.get("/health", (_req, res) => res.json({ ok: true, service: "multiplayer" }));
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: corsOrigin === "*" ? true : corsOrigin.split(",").map((s) => s.trim()) },
  connectTimeout: 45_000,
  pingTimeout: 25_000,
  pingInterval: 10_000,
});
/** key -> [{ socket, uid, mode, betAmount }] */
const waitingQueues = new Map();
/** room -> game state */
const roomStates = new Map();
function perPlayerMsForMode(mode) {
  return mode === "paid" ? 5 * 60 * 1000 : 10 * 60 * 1000;
}
function queueKey(mode, betAmount) {
  return mode === "paid" ? `paid_${Number(betAmount) || 0}` : "free";
}
function createRoomState(room, gameId, mode, betAmount, whiteUid, blackUid) {
  const per = perPlayerMsForMode(mode);
  const now = Date.now();
  const st = {
    room,
    gameId,
    mode,
    betAmount,
    chess: new Chess(),
    whiteUid,
    blackUid,
    // Required fields
    whiteTime: per,
    blackTime: per,
    currentTurn: "white",
    winner: null,
    gameOver: false,
    // Internal timing state
    turnStartedAt: now,
    lastTickAt: now,
    // Safety / cleanup
    isFinished: false,
    disconnectTimeoutId: null,
  };
  roomStates.set(room, st);
  return st;
}
function toClockPayload(st) {
  return {
    whiteTime: Math.max(0, st.whiteTime),
    blackTime: Math.max(0, st.blackTime),
  };
}
function emitTimerUpdate(st) {
  io.to(st.room).emit("timerUpdate", toClockPayload(st));
}
function clearDisconnectGrace(st) {
  if (st.disconnectTimeoutId) {
    clearTimeout(st.disconnectTimeoutId);
    st.disconnectTimeoutId = null;
  }
}
function cleanupRoom(room) {
  const st = roomStates.get(room);
  if (!st) return;
  clearDisconnectGrace(st);
  roomStates.delete(room);
}
function finalizeGame(room, { winner = null, reason = "game ended", draw = false }) {
  const st = roomStates.get(room);
  if (!st || st.gameOver || st.isFinished) return;
  st.gameOver = true;
  st.isFinished = true;
  st.winner = winner; // "white" | "black" | null
  clearDisconnectGrace(st);
  // Required event payload
  io.to(room).emit("gameOver", {
    winner: st.winner,
    reason,
  });
  // Keep compatibility with your existing clients
  io.to(room).emit("gameResult", {
    room: st.room,
    gameId: st.gameId,
    mode: st.mode,
    betAmount: st.betAmount,
    whiteUid: st.whiteUid,
    blackUid: st.blackUid,
    draw: !!draw,
    winnerColor: st.winner,
    reason,
    paid: st.mode === "paid" && st.betAmount > 0,
    balancesByUid: null,
  });
  cleanupRoom(room);
}
function handleTimeout(room, loserColor) {
  // loserColor: "white" | "black"
  if (loserColor === "white") {
    finalizeGame(room, {
      winner: "black",
      reason: "White ran out of time",
      draw: false,
    });
  } else {
    finalizeGame(room, {
      winner: "white",
      reason: "Black ran out of time",
      draw: false,
    });
  }
}
/**
 * Tick all active games once/sec:
 * - only currentTurn player's clock decreases
 * - emits timerUpdate every second
 * - auto gameOver on timeout
 */
function tickActiveGames() {
  const now = Date.now();
  for (const [room, st] of roomStates.entries()) {
    if (!st || st.gameOver || st.isFinished) continue;
    const elapsed = now - st.lastTickAt;
    if (elapsed < 1000) continue;
    const steps = Math.floor(elapsed / 1000); // catch-up if server lagged
    st.lastTickAt += steps * 1000;
    if (st.currentTurn === "white") {
      st.whiteTime = Math.max(0, st.whiteTime - steps * 1000);
      if (st.whiteTime <= 0) {
        handleTimeout(room, "white");
        continue;
      }
    } else {
      st.blackTime = Math.max(0, st.blackTime - steps * 1000);
      if (st.blackTime <= 0) {
        handleTimeout(room, "black");
        continue;
      }
    }
    emitTimerUpdate(st);
  }
}
function playerColorFromSocketMeta(meta) {
  if (!meta) return null;
  if (meta.color === "white") return "white";
  if (meta.color === "black") return "black";
  return null;
}
function startDisconnectGrace(room, disconnectedColor) {
  const st = roomStates.get(room);
  if (!st || st.gameOver || st.isFinished) return;
  clearDisconnectGrace(st);
  st.disconnectTimeoutId = setTimeout(() => {
    const current = roomStates.get(room);
    if (!current || current.gameOver || current.isFinished) return;
    const winner = disconnectedColor === "white" ? "black" : "white";
    finalizeGame(room, {
      winner,
      reason: "Opponent disconnected",
      draw: false,
    });
  }, 30_000);
}
io.on("connection", (socket) => {
  socket.on("joinGame", (payload = {}) => {
    const mode = payload.mode === "paid" ? "paid" : "free";
    const betAmount = mode === "paid" ? Math.max(0, Math.floor(Number(payload.betAmount) || 0)) : 0;
    const uid = typeof payload.uid === "string" && payload.uid.length > 0 ? payload.uid : socket.id;
    if (mode === "paid" && betAmount <= 0) {
      socket.emit("matchmaking_error", { message: "Invalid stake for paid match." });
      return;
    }
    const key = queueKey(mode, betAmount);
    if (!waitingQueues.has(key)) waitingQueues.set(key, []);
    const queue = waitingQueues.get(key);
    if (queue.length === 0) {
      queue.push({ socket, uid, mode, betAmount });
      socket.waitingQueueKey = key;
      socket.emit("waiting");
      return;
    }
    const first = queue.shift();
    if (!first?.socket?.connected || first.socket.id === socket.id) {
      socket.emit("matchmaking_error", { message: "Queue entry expired. Try again." });
      return;
    }
    const room = `${first.socket.id}#${socket.id}`;
    const gameId = crypto.randomUUID();
    const whiteUid = first.uid;
    const blackUid = uid;
    first.socket.join(room);
    socket.join(room);
    first.socket.room = room;
    socket.room = room;
    first.socket.matchMeta = { room, color: "white", gameId, mode, betAmount, playerId: whiteUid };
    socket.matchMeta = { room, color: "black", gameId, mode, betAmount, playerId: blackUid };
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
      clock: toClockPayload(st),
      gameOver: st.gameOver,
      winner: st.winner,
    });
    first.socket.emit("start", startPayload("white"));
    socket.emit("start", startPayload("black"));
    emitTimerUpdate(st);
  });
  socket.on("rejoinMatch", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : "";
    const uid = typeof payload.uid === "string" ? payload.uid : "";
    if (!room || !uid) {
      socket.emit("rejoinFailed", { message: "Invalid rejoin payload." });
      return;
    }
    const st = roomStates.get(room);
    if (!st || st.gameOver || st.isFinished) {
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
      mode: st.mode,
      betAmount: st.betAmount,
      playerId: uid,
    };
    clearDisconnectGrace(st);
    socket.emit("rejoinOk", {
      room,
      color,
      gameId: st.gameId,
      mode: st.mode,
      betAmount: st.betAmount,
      whiteUid: st.whiteUid,
      blackUid: st.blackUid,
      fen: st.chess.fen(),
      moves: st.chess.history(),
      currentTurn: st.currentTurn,
      clock: toClockPayload(st),
      gameOver: st.gameOver,
      winner: st.winner,
    });
    emitTimerUpdate(st);
  });
  socket.on("move", (data = {}) => {
    const room = typeof data.room === "string" ? data.room : "";
    if (!room || socket.room !== room || !data?.move) return;
    const st = roomStates.get(room);
    if (!st || st.gameOver || st.isFinished || !socket.matchMeta) return;
    const mover = playerColorFromSocketMeta(socket.matchMeta);
    if (!mover || mover !== st.currentTurn) return;
    const moveTry = st.chess.move({
      from: data.move.from,
      to: data.move.to,
      promotion: data.move.promotion || "q",
    });
    if (!moveTry) return;
    // Turn switch (clock ticking is handled by global timer)
    st.currentTurn = st.currentTurn === "white" ? "black" : "white";
    st.turnStartedAt = Date.now();
    io.to(room).emit("move", data.move);
    emitTimerUpdate(st);
    if (st.chess.isCheckmate()) {
      const loser = st.chess.turn() === "w" ? "white" : "black";
      const winner = loser === "white" ? "black" : "white";
      finalizeGame(room, {
        winner,
        reason: "Checkmate",
        draw: false,
      });
      return;
    }
    if (st.chess.isDraw()) {
      finalizeGame(room, {
        winner: null,
        reason: "Draw",
        draw: true,
      });
    }
  });
  socket.on("playerResigned", () => {
    const meta = socket.matchMeta;
    if (!meta?.room || !meta?.color) return;
    const winner = meta.color === "white" ? "black" : "white";
    finalizeGame(meta.room, {
      winner,
      reason: "Player resigned",
      draw: false,
    });
  });
  socket.on("resign", () => {
    const meta = socket.matchMeta;
    if (!meta?.room || !meta?.color) return;
    const winner = meta.color === "white" ? "black" : "white";
    finalizeGame(meta.room, {
      winner,
      reason: "Player resigned",
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
    // Start disconnect grace if player was in active room
    const meta = socket.matchMeta;
    if (meta?.room && meta?.color) {
      const st = roomStates.get(meta.room);
      if (st && !st.gameOver && !st.isFinished) {
        startDisconnectGrace(meta.room, meta.color);
      }
    }
  });
});
/** Single global interval prevents duplicate room intervals */
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
