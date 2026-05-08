const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { Chess } = require("chess.js");
const { Server } = require("socket.io");
const cors = require("cors");
const app = express();
const corsOrigin = process.env.CORS_ORIGIN || "*";
const adminApiKey = (process.env.ADMIN_API_KEY || "").trim();
app.set("trust proxy", 1);
app.use(express.json({ limit: "200kb" }));
app.use(
  cors({
    origin: corsOrigin === "*" ? true : corsOrigin.split(",").map((s) => s.trim()),
  })
);
app.get("/", (_req, res) => {
  res.type("text").send("Chess socket server OK");
});
app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "chess-socket" });
});
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: corsOrigin === "*" ? true : corsOrigin.split(",").map((s) => s.trim()) },
  connectTimeout: 45_000,
  pingTimeout: 25_000,
  pingInterval: 10_000,
});
/** @type {Map<string, { socket: import("socket.io").Socket, uid: string }>} */
const firestoreMatchWaiters = new Map();
/** @type {Map<string, Array<{ socket: import("socket.io").Socket, uid: string, mode: string, betAmount: number }>>} */
const waitingQueues = new Map();
/** @type {Map<string, any>} */
const roomStates = new Map();
const DEFAULT_SERVER_BALANCE = 1000;
/** @type {Map<string, number>} */
const serverBalancesByUid = new Map();
/** @type {Map<string, Array<any>>} */
const serverWalletLedgerByUid = new Map();
/** Paid games that already updated the server ledger (idempotent). */
const serverWalletSettledGameIds = new Set();
function perPlayerMsForMode(mode) {
  return mode === "paid" ? 5 * 60 * 1000 : 10 * 60 * 1000;
}
function normalizeUid(uid) {
  if (uid == null) return "";
  return String(uid).trim();
}
function getOrCreateLedger(uid) {
  if (!serverWalletLedgerByUid.has(uid)) {
    serverWalletLedgerByUid.set(uid, []);
  }
  return serverWalletLedgerByUid.get(uid);
}
function pushWalletLedger(uid, entry) {
  const rows = getOrCreateLedger(uid);
  rows.push({
    id: crypto.randomUUID(),
    uid,
    at: Date.now(),
    ...entry,
  });
  if (rows.length > 250) {
    rows.splice(0, rows.length - 250);
  }
}
function getSrvBal(uidRaw) {
  const uid = normalizeUid(uidRaw);
  if (!uid) return DEFAULT_SERVER_BALANCE;
  if (!serverBalancesByUid.has(uid)) {
    serverBalancesByUid.set(uid, DEFAULT_SERVER_BALANCE);
  }
  return serverBalancesByUid.get(uid);
}
function setSrvBal(uidRaw, nextBalance, meta = {}) {
  const uid = normalizeUid(uidRaw);
  if (!uid) return null;
  const prev = getSrvBal(uid);
  const next = Math.max(0, Math.floor(Number(nextBalance) || 0));
  serverBalancesByUid.set(uid, next);
  pushWalletLedger(uid, {
    type: "set",
    source: meta.source || "system",
    reason: meta.reason || "manual_set",
    prevBalance: prev,
    nextBalance: next,
    delta: next - prev,
    gameId: meta.gameId || null,
    by: meta.by || null,
    note: meta.note || null,
    ref: meta.ref || null,
  });
  return next;
}
function applySrvDelta(uidRaw, deltaRaw, meta = {}) {
  const uid = normalizeUid(uidRaw);
  if (!uid) return null;
  const delta = Math.floor(Number(deltaRaw) || 0);
  const prev = getSrvBal(uid);
  const next = Math.max(0, prev + delta);
  serverBalancesByUid.set(uid, next);
  pushWalletLedger(uid, {
    type: delta >= 0 ? "credit" : "debit",
    source: meta.source || "system",
    reason: meta.reason || "adjustment",
    prevBalance: prev,
    nextBalance: next,
    delta,
    gameId: meta.gameId || null,
    by: meta.by || null,
    note: meta.note || null,
    ref: meta.ref || null,
  });
  return next;
}
function parseMoneyAmount(value) {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return null;
  return n;
}
function isAdminRequestAuthorized(req) {
  if (!adminApiKey) return false;
  const header = req.get("x-admin-key");
  if (header && String(header).trim() === adminApiKey) {
    return true;
  }
  const auth = req.get("authorization");
  if (!auth) return false;
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  return token === adminApiKey;
}
function requireAdmin(req, res, next) {
  if (!adminApiKey) {
    res.status(503).json({
      ok: false,
      error: "ADMIN_API_KEY is not configured on the server.",
    });
    return;
  }
  if (!isAdminRequestAuthorized(req)) {
    res.status(401).json({ ok: false, error: "Unauthorized admin request." });
    return;
  }
  next();
}
function calculateWinnerCreditServer(betPerPlayer) {
  const total = betPerPlayer * 2;
  const commission = Math.floor(total * 0.1);
  const winnerAmount = total - commission;
  return { total, commission, winnerAmount };
}
app.get("/admin/wallet/:uid", requireAdmin, (req, res) => {
  const uid = normalizeUid(req.params.uid);
  if (!uid) {
    res.status(400).json({ ok: false, error: "uid is required." });
    return;
  }
  res.json({
    ok: true,
    uid,
    balance: getSrvBal(uid),
    ledgerCount: getOrCreateLedger(uid).length,
  });
});
app.get("/admin/wallet/:uid/transactions", requireAdmin, (req, res) => {
  const uid = normalizeUid(req.params.uid);
  if (!uid) {
    res.status(400).json({ ok: false, error: "uid is required." });
    return;
  }
  const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 50));
  const rows = [...getOrCreateLedger(uid)].slice(-limit).reverse();
  res.json({ ok: true, uid, transactions: rows });
});
app.post("/admin/wallet/credit", requireAdmin, (req, res) => {
  const uid = normalizeUid(req.body?.uid);
  const amount = parseMoneyAmount(req.body?.amount);
  if (!uid || amount == null) {
    res.status(400).json({ ok: false, error: "uid and positive amount are required." });
    return;
  }
  const next = applySrvDelta(uid, amount, {
    source: "admin_panel",
    reason: "admin_credit",
    by: req.body?.by || "admin",
    note: req.body?.note || null,
    ref: req.body?.ref || req.body?.upiRef || null,
  });
  res.json({ ok: true, uid, balance: next });
});
app.post("/admin/wallet/debit", requireAdmin, (req, res) => {
  const uid = normalizeUid(req.body?.uid);
  const amount = parseMoneyAmount(req.body?.amount);
  if (!uid || amount == null) {
    res.status(400).json({ ok: false, error: "uid and positive amount are required." });
    return;
  }
  const next = applySrvDelta(uid, -amount, {
    source: "admin_panel",
    reason: "admin_debit",
    by: req.body?.by || "admin",
    note: req.body?.note || null,
    ref: req.body?.ref || null,
  });
  res.json({ ok: true, uid, balance: next });
});
app.post("/admin/wallet/set", requireAdmin, (req, res) => {
  const uid = normalizeUid(req.body?.uid);
  const nextRaw = Math.floor(Number(req.body?.balance));
  if (!uid || !Number.isFinite(nextRaw) || nextRaw < 0) {
    res.status(400).json({ ok: false, error: "uid and non-negative balance are required." });
    return;
  }
  const next = setSrvBal(uid, nextRaw, {
    source: "admin_panel",
    reason: "admin_set_balance",
    by: req.body?.by || "admin",
    note: req.body?.note || null,
    ref: req.body?.ref || null,
  });
  res.json({ ok: true, uid, balance: next });
});
function buildBalancesForGameResult(st, payload) {
  const snapshot = () => ({
    [st.whiteUid]: getSrvBal(st.whiteUid),
    [st.blackUid]: getSrvBal(st.blackUid),
  });
  if (st.mode !== "paid" || !(st.betAmount > 0)) {
    return { balancesByUid: snapshot(), paid: false };
  }
  const gid = st.gameId;
  if (serverWalletSettledGameIds.has(gid)) {
    return { balancesByUid: snapshot(), paid: true, alreadySettled: true };
  }
  serverWalletSettledGameIds.add(gid);
  if (payload.draw) {
    pushWalletLedger(st.whiteUid, {
      type: "settlement",
      source: "match_result",
      reason: "draw",
      prevBalance: getSrvBal(st.whiteUid),
      nextBalance: getSrvBal(st.whiteUid),
      delta: 0,
      gameId: st.gameId,
      note: "Paid match draw: no wallet movement",
    });
    pushWalletLedger(st.blackUid, {
      type: "settlement",
      source: "match_result",
      reason: "draw",
      prevBalance: getSrvBal(st.blackUid),
      nextBalance: getSrvBal(st.blackUid),
      delta: 0,
      gameId: st.gameId,
      note: "Paid match draw: no wallet movement",
    });
    return { balancesByUid: snapshot(), paid: true };
  }
  const bet = st.betAmount;
  const { winnerAmount } = calculateWinnerCreditServer(bet);
  const prevW = getSrvBal(st.whiteUid);
  const prevB = getSrvBal(st.blackUid);
  let w = prevW;
  let b = prevB;
  if (payload.winnerColor === "white") {
    w = Math.max(0, w - bet + winnerAmount);
    b = Math.max(0, b - bet);
  } else if (payload.winnerColor === "black") {
    b = Math.max(0, b - bet + winnerAmount);
    w = Math.max(0, w - bet);
  }
  serverBalancesByUid.set(st.whiteUid, w);
  serverBalancesByUid.set(st.blackUid, b);
  pushWalletLedger(st.whiteUid, {
    type: "settlement",
    source: "match_result",
    reason: payload.reason || "game_end",
    prevBalance: prevW,
    nextBalance: w,
    delta: w - prevW,
    gameId: st.gameId,
    note: `Match settlement (${payload.winnerColor || "draw"})`,
  });
  pushWalletLedger(st.blackUid, {
    type: "settlement",
    source: "match_result",
    reason: payload.reason || "game_end",
    prevBalance: prevB,
    nextBalance: b,
    delta: b - prevB,
    gameId: st.gameId,
    note: `Match settlement (${payload.winnerColor || "draw"})`,
  });
  return { balancesByUid: { [st.whiteUid]: w, [st.blackUid]: b }, paid: true };
}
/**
 * Live clock for clients — remaining ms after current turn's elapsed time.
 * Internal `st.whiteMs` / `st.blackMs` stay as banks at turn start; never send raw banks without elapsed.
 */
function liveClockPayloadFromState(st) {
  const serverNow = Date.now();
  const elapsed = Math.max(0, serverNow - st.turnClockStartedAt);
  let white = st.whiteMs;
  let black = st.blackMs;
  if (st.toMove === "w") {
    white = Math.max(0, white - elapsed);
  } else {
    black = Math.max(0, black - elapsed);
  }
  return {
    room: st.room,
    gameId: st.gameId,
    whiteRemainingMs: white,
    blackRemainingMs: black,
    whiteTime: white,
    blackTime: black,
    currentTurn: st.toMove === "w" ? "white" : "black",
    sideToMove: st.toMove,
    turnClockStartedAt: st.turnClockStartedAt,
    lastMoveTimestamp: st.turnClockStartedAt,
    serverNow,
  };
}
function clearDisconnectGrace(st) {
  if (st.disconnectTimeoutId) {
    clearTimeout(st.disconnectTimeoutId);
    st.disconnectTimeoutId = null;
  }
  st.disconnectingColor = null;
  st.disconnectGraceEndsAt = null;
}
/** Remaining reconnect window (seconds); clients show AFK banner from this. */
function broadcastPlayerAfk(room, st) {
  if (!st.disconnectingColor || !st.disconnectGraceEndsAt) return;
  const sec = Math.max(0, Math.ceil((st.disconnectGraceEndsAt - Date.now()) / 1000));
  io.to(room).emit("playerAFK", {
    player: st.disconnectingColor,
    remainingTime: sec,
  });
}
function destroyRoomState(room) {
  const st = roomStates.get(room);
  if (!st) return;
  clearDisconnectGrace(st);
  roomStates.delete(room);
}
function endMatch(room, payload) {
  const st = roomStates.get(room);
  if (!st || st.isFinished) return;
  st.isFinished = true;
  clearDisconnectGrace(st);
  const { balancesByUid, paid } = buildBalancesForGameResult(st, payload);
  const out = {
    room,
    gameId: st.gameId,
    paid,
    draw: !!payload.draw,
    winnerColor: payload.winnerColor ?? null,
    reason: payload.reason,
    balancesByUid,
    whiteUid: st.whiteUid,
    blackUid: st.blackUid,
    betAmount: st.betAmount,
    mode: st.mode,
  };
  roomStates.delete(room);
  io.to(room).emit("gameResult", out);
}
/** Side to move ran out of time — opponent wins (blitz flag). */
function finalizeTimeoutFlag(room) {
  const st = roomStates.get(room);
  if (!st || st.isFinished) return;
  const now = Date.now();
  const bank = st.toMove === "w" ? st.whiteMs : st.blackMs;
  const elapsed = Math.max(0, now - st.turnClockStartedAt);
  if (bank - elapsed > 0) return;
  const winnerColor = st.toMove === "w" ? "black" : "white";
  endMatch(room, {
    reason: "timeout",
    winnerColor,
    draw: false,
  });
}
/** 1 Hz: broadcast live clocks + detect flag fall (server-only time). */
function tickActiveGames() {
  for (const [room, st] of [...roomStates.entries()]) {
    if (!st || st.isFinished || !roomStates.has(room)) continue;
    const live = liveClockPayloadFromState(st);
    const w = live.whiteRemainingMs;
    const b = live.blackRemainingMs;
    if (st.toMove === "w" && w <= 0) {
      finalizeTimeoutFlag(room);
      continue;
    }
    if (st.toMove === "b" && b <= 0) {
      finalizeTimeoutFlag(room);
      continue;
    }
    if (st.disconnectingColor) {
      broadcastPlayerAfk(room, st);
    }
    io.to(room).emit("clockSync", live);
    io.to(room).emit("gameState", live);
  }
}
function startDisconnectGrace(room, disconnectedColor) {
  const st = roomStates.get(room);
  if (!st || st.isFinished) return;
  clearDisconnectGrace(st);
  st.disconnectingColor = disconnectedColor;
  const GRACE_MS = 120_000;
  st.disconnectGraceEndsAt = Date.now() + GRACE_MS;
  broadcastPlayerAfk(room, st);
  st.disconnectTimeoutId = setTimeout(() => {
    st.disconnectTimeoutId = null;
    if (!roomStates.has(room)) return;
    const s2 = roomStates.get(room);
    if (!s2 || s2.isFinished) return;
    const winnerColor = disconnectedColor === "white" ? "black" : "white";
    endMatch(room, {
      reason: "disconnect_forfeit",
      winnerColor,
      draw: false,
    });
  }, GRACE_MS);
}
function createRoomState(room, gameId, mode, betAmount, whiteUid, blackUid) {
  destroyRoomState(room);
  const per = perPlayerMsForMode(mode);
  const turnClockStartedAt = Date.now();
  const st = {
    room,
    gameId,
    mode,
    betAmount,
    isFinished: false,
    chess: new Chess(),
    whiteUid,
    blackUid,
    whiteMs: per,
    blackMs: per,
    toMove: "w",
    turnClockStartedAt,
    disconnectTimeoutId: null,
    disconnectingColor: null,
    disconnectGraceEndsAt: null,
  };
  roomStates.set(room, st);
  return st;
}
function queueKey(mode, betAmount) {
  if (mode === "paid") return `paid_${Number(betAmount) || 0}`;
  return "free";
}
function moverCharFromMeta(meta) {
  if (!meta || (meta.color !== "white" && meta.color !== "black")) return null;
  return meta.color === "white" ? "w" : "b";
}
function handlePlayerResigned(socket, payload = {}) {
  const room = socket.matchMeta?.room;
  if (!room || socket.room !== room || !socket.matchMeta) return;
  if (socket.matchMeta.room !== room) return;
  const st = roomStates.get(room);
  if (!st || st.isFinished) return;
  const reqGid = payload.gameId != null ? String(payload.gameId) : "";
  const reqPid = payload.playerId != null ? String(payload.playerId) : "";
  if (reqGid && reqGid !== st.gameId) return;
  if (reqPid && socket.matchMeta.playerId && reqPid !== socket.matchMeta.playerId) return;
  const loserColor = socket.matchMeta.color;
  const winnerColor = loserColor === "white" ? "black" : "white";
  endMatch(room, {
    reason: "playerResigned",
    winnerColor,
    draw: false,
  });
}
io.on("connection", (socket) => {
  console.log("Player connected:", socket.id);
  socket.on("joinGame", (payload = {}) => {
    if (payload.firestoreMatchId) {
      const rid = String(payload.firestoreMatchId)
        .replace(/[^a-zA-Z0-9_-]/g, "")
        .slice(0, 120);
      if (!rid) {
        socket.emit("matchmaking_error", { message: "Invalid match id." });
        return;
      }
      const mode = payload.mode === "paid" ? "paid" : "free";
      const betAmount =
        mode === "paid" ? Math.max(0, Math.floor(Number(payload.betAmount) || 0)) : 0;
      const uid =
        typeof payload.uid === "string" && payload.uid.length > 0 ? payload.uid : socket.id;
      if (mode === "paid" && betAmount <= 0) {
        socket.emit("matchmaking_error", {
          message: "Invalid stake for a money match.",
        });
        return;
      }
      const room = `fs_${rid}`;
      if (!firestoreMatchWaiters.has(rid)) {
        firestoreMatchWaiters.set(rid, { socket, uid });
        socket.join(room);
        socket.room = room;
        socket.firestoreMatchWaiterKey = rid;
        socket.emit("waiting");
        return;
      }
      const first = firestoreMatchWaiters.get(rid);
      firestoreMatchWaiters.delete(rid);
      if (!first?.socket?.connected || first.socket.id === socket.id) {
        socket.emit("matchmaking_error", {
          message: "Match room expired. Try again.",
        });
        return;
      }
      const gameId = crypto.randomUUID();
      const whiteUid = first.uid;
      const blackUid = uid;
      first.socket.join(room);
      socket.join(room);
      first.socket.room = room;
      socket.room = room;
      first.socket.firestoreMatchWaiterKey = undefined;
      first.socket.matchMeta = {
        room,
        color: "white",
        gameId,
        betAmount,
        mode,
        playerId: whiteUid,
      };
      socket.matchMeta = {
        room,
        color: "black",
        gameId,
        betAmount,
        mode,
        playerId: blackUid,
      };
      createRoomState(room, gameId, mode, betAmount, whiteUid, blackUid);
      const st = roomStates.get(room);
      const startPayload = (color) => ({
        room,
        color,
        gameId,
        betAmount: mode === "paid" ? betAmount : 0,
        mode,
        whiteUid,
        blackUid,
        clock: liveClockPayloadFromState(st),
      });
      first.socket.emit("start", startPayload("white"));
      socket.emit("start", startPayload("black"));
      return;
    }
    const mode = payload.mode === "paid" ? "paid" : "free";
    const betAmount =
      mode === "paid" ? Math.max(0, Math.floor(Number(payload.betAmount) || 0)) : 0;
    const uid =
      typeof payload.uid === "string" && payload.uid.length > 0 ? payload.uid : socket.id;
    if (mode === "paid" && betAmount <= 0) {
      socket.emit("matchmaking_error", {
        message: "Invalid stake for a money match.",
      });
      return;
    }
    const guestLike =
      typeof payload.uid === "string" && payload.uid.startsWith("guest_");
    if (mode === "paid" && (!payload.uid || (payload.uid === socket.id && !guestLike))) {
      socket.emit("matchmaking_error", {
        message: "Use a guest id for paid test matches.",
      });
      return;
    }
    const key = queueKey(mode, betAmount);
    if (!waitingQueues.has(key)) {
      waitingQueues.set(key, []);
    }
    const queue = waitingQueues.get(key);
    const entry = { socket, uid, mode, betAmount };
    if (queue.length > 0) {
      const waitingPlayer = queue.shift();
      const room = `${waitingPlayer.socket.id}#${socket.id}`;
      const gameId = crypto.randomUUID();
      const whiteUid = waitingPlayer.uid;
      const blackUid = entry.uid;
      socket.join(room);
      waitingPlayer.socket.join(room);
      socket.room = room;
      waitingPlayer.socket.room = room;
      waitingPlayer.socket.matchMeta = {
        room,
        color: "white",
        gameId,
        betAmount,
        mode,
        playerId: whiteUid,
      };
      socket.matchMeta = {
        room,
        color: "black",
        gameId,
        betAmount,
        mode,
        playerId: blackUid,
      };
      createRoomState(room, gameId, mode, betAmount, whiteUid, blackUid);
      const st = roomStates.get(room);
      const startPayload = (color) => ({
        room,
        color,
        gameId,
        betAmount: mode === "paid" ? betAmount : 0,
        mode,
        whiteUid,
        blackUid,
        clock: liveClockPayloadFromState(st),
      });
      waitingPlayer.socket.emit("start", startPayload("white"));
      socket.emit("start", startPayload("black"));
    } else {
      queue.push(entry);
      socket.waitingQueueKey = key;
      socket.emit("waiting");
    }
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
    if (!st || st.isFinished) {
      socket.emit("rejoinFailed", { message: "Match not active." });
      return;
    }
    if (uid !== st.whiteUid && uid !== st.blackUid) {
      socket.emit("rejoinFailed", { message: "Not a player in this match." });
      return;
    }
    const color = uid === st.whiteUid ? "white" : "black";
    const returnedFromDisconnect = st.disconnectingColor === color;
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
    clearDisconnectGrace(st);
    if (returnedFromDisconnect) {
      io.to(room).emit("playerReturned", { player: color });
    }
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
      clock: liveClockPayloadFromState(st),
    });
    const liveRejoin = liveClockPayloadFromState(st);
    io.to(room).emit("clockSync", liveRejoin);
    io.to(room).emit("gameState", liveRejoin);
  });
  socket.on("appBackgrounded", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : "";
    const player =
      payload.player === "white" || payload.player === "black"
        ? payload.player
        : null;
    if (!room || !player || !socket.matchMeta) {
      return;
    }
    if (socket.matchMeta.room !== room || socket.matchMeta.color !== player) {
      return;
    }
    const st = roomStates.get(room);
    if (!st || st.isFinished) {
      return;
    }
    // Start/restart reconnect grace and AFK countdown for opponent
    startDisconnectGrace(room, player);
  });
  socket.on("appForegrounded", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : "";
    const player =
      payload.player === "white" || payload.player === "black"
        ? payload.player
        : null;
    if (!room || !player || !socket.matchMeta) {
      return;
    }
    if (socket.matchMeta.room !== room || socket.matchMeta.color !== player) {
      return;
    }
    const st = roomStates.get(room);
    if (!st || st.isFinished) {
      return;
    }
    const returnedFromAway = st.disconnectingColor === player;
    clearDisconnectGrace(st);
    if (returnedFromAway) {
      io.to(room).emit("playerReturned", { player });
    }
  });
  socket.on("move", (data = {}) => {
    if (!data?.move) {
      return;
    }
    const room = typeof data.room === "string" ? data.room : "";
    if (!room || socket.room !== room || !socket.matchMeta) return;
    const st = roomStates.get(room);
    if (!st || st.isFinished) return;
    const mover = moverCharFromMeta(socket.matchMeta);
    if (!mover || mover !== st.toMove) return;
    const now = Date.now();
    const bank = st.toMove === "w" ? st.whiteMs : st.blackMs;
    const elapsed = Math.max(0, now - st.turnClockStartedAt);
    if (elapsed >= bank) {
      finalizeTimeoutFlag(room);
      return;
    }
    const moveTry = st.chess.move({
      from: data.move.from,
      to: data.move.to,
      promotion: data.move.promotion || "q",
    });
    if (!moveTry) return;
    if (st.toMove === "w") {
      st.whiteMs = bank - elapsed;
    } else {
      st.blackMs = bank - elapsed;
    }
    st.toMove = st.toMove === "w" ? "b" : "w";
    st.turnClockStartedAt = now;
    io.to(room).emit("move", data.move);
    const liveAfter = liveClockPayloadFromState(st);
    io.to(room).emit("clockSync", liveAfter);
    io.to(room).emit("gameState", liveAfter);
    if (st.chess.isCheckmate()) {
      const loser = st.chess.turn();
      const winnerColor = loser === "w" ? "black" : "white";
      endMatch(room, {
        reason: "checkmate",
        winnerColor,
        draw: false,
      });
      return;
    }
    if (st.chess.isDraw()) {
      endMatch(room, {
        reason: "draw",
        draw: true,
        winnerColor: null,
      });
    }
  });
  socket.on("playerResigned", (payload) => handlePlayerResigned(socket, payload));
  socket.on("resign", (payload) => handlePlayerResigned(socket, payload));
  socket.on("disconnect", () => {
    console.log("Player disconnected");
    const fsKey = socket.firestoreMatchWaiterKey;
    if (fsKey && firestoreMatchWaiters.get(fsKey)?.socket === socket) {
      firestoreMatchWaiters.delete(fsKey);
    }
    const meta = socket.matchMeta;
    if (meta?.room && meta.color) {
      const st = roomStates.get(meta.room);
      if (st && !st.isFinished) {
        startDisconnectGrace(meta.room, meta.color);
      }
    }
    const key = socket.waitingQueueKey;
    if (!key) return;
    const queue = waitingQueues.get(key);
    if (!queue) return;
    const idx = queue.findIndex((e) => e.socket === socket);
    if (idx >= 0) {
      queue.splice(idx, 1);
    }
    if (queue.length === 0) {
      waitingQueues.delete(key);
    }
  });
});
setInterval(tickActiveGames, 1000);
const PORT = Number(process.env.PORT) || 4000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`Chess socket server listening on ${PORT}`);
});
