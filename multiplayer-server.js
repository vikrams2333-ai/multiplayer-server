### FILE: index.js ###
/**
 * Chess realtime server — run on Hostinger VPS (or any Node host).
 * Start from `server/`: `npm start` (uses process.env.PORT, default 4000).
 *
 * Env:
 *   MONGODB_URI — optional; without it, paid wallets use in-memory fallback only.
 *   JWT_SECRET — required for Socket.IO `auth.token` (login users); issuer `chess-bet-app`.
 *   SOCKET_REQUIRE_JWT_FOR_PAID — set `true` to block paid queue unless socket is authenticated.
 *   ADMIN_API_KEY — admin HTTP routes (`x-admin-key` or `Authorization: Bearer ...`).
 *
 * Authoritative: position (chess.js), clocks, match end, wallets via `gameResult` payloads only.
 */
try {
  require("dotenv").config();
} catch {
  /* optional */
}

const express = require("express");
const http = require("http");
const crypto = require("crypto");
const { Chess } = require("chess.js");
const { Server } = require("socket.io");
const cors = require("cors");
const mongoose = require("mongoose");
const jwt = require("jsonwebtoken");
const User = require("./models/User");
const GameSettlement = require("./models/GameSettlement");
const env = require("./src/config/env");
const { registerApiRoutes } = require("./src/routes");

const app = express();
const corsOrigin = process.env.CORS_ORIGIN || "*";
const adminApiKey = (process.env.ADMIN_API_KEY || "").trim();
app.set("trust proxy", 1);
app.use(express.json({ limit: "200kb" }));
app.use(
  cors({
    origin: corsOrigin === "*" ? true : corsOrigin.split(",").map((s) => s.trim()),
    credentials: true,
  })
);
registerApiRoutes(app);
app.get("/", (_req, res) => {
  res.type("text").send("Chess socket server OK");
});
app.get("/health", (_req, res) => {
  const mongoOk = mongoose.connection.readyState === 1;
  res.json({
    ok: true,
    service: "chess-socket",
    mongo: mongoOk ? "connected" : "disconnected",
  });
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: corsOrigin === "*" ? true : corsOrigin.split(",").map((s) => s.trim()) },
  connectTimeout: 45_000,
  pingTimeout: 25_000,
  pingInterval: 10_000,
});

/**
 * Optional JWT on the Socket.IO handshake (`auth: { token }` from the app).
 * When valid, paid/free identity is taken from the database user — clients cannot spoof `uid`.
 * Invalid non-empty tokens reject the connection (prevents “fake login”).
 */
io.use((socket, next) => {
  socket.verifiedUserId = null;
  const tokenRaw =
    (socket.handshake.auth && socket.handshake.auth.token) ||
    (socket.handshake.query && socket.handshake.query.token) ||
    "";
  const token = String(tokenRaw || "").trim();
  if (!token) {
    return next();
  }
  if (!env.jwtSecret) {
    return next(new Error("Server misconfiguration: JWT_SECRET missing."));
  }
  try {
    const payload = jwt.verify(token, env.jwtSecret, { issuer: "chess-bet-app" });
    const sub = String(payload.sub || "").trim();
    if (!sub) {
      return next(new Error("Unauthorized: empty token subject."));
    }
    socket.verifiedUserId = sub;
    return next();
  } catch (_err) {
    return next(new Error("Unauthorized: invalid auth token."));
  }
});

/** @type {Map<string, { socket: import("socket.io").Socket, uid: string }>} */
const firestoreMatchWaiters = new Map();

/** @type {Map<string, Array<{ socket: import("socket.io").Socket, uid: string, mode: string, betAmount: number }>>} */
const waitingQueues = new Map();

function perPlayerMsForMode(mode) {
  return mode === "paid" ? 5 * 60 * 1000 : 10 * 60 * 1000;
}

/** @type {Map<string, any>} */
const roomStates = new Map();

const DEFAULT_SERVER_BALANCE = 1000;
/** @type {Map<string, number>} */
const serverBalancesByUid = new Map();
/** @type {Map<string, Array<any>>} */
const serverWalletLedgerByUid = new Map();
/** Paid games that already updated the server ledger (idempotent). */
const serverWalletSettledGameIds = new Set();

function normalizeUid(uid) {
  if (uid == null) return "";
  return String(uid).trim();
}

function mongoConnected() {
  return mongoose.connection.readyState === 1;
}

const K_RATING = 32;

function expectedScore(rA, rB) {
  return 1 / (1 + Math.pow(10, (rB - rA) / 400));
}

async function snapshotBalancesFromMongo(whiteUid, blackUid) {
  const w = normalizeUid(whiteUid);
  const b = normalizeUid(blackUid);
  const [wa, ba] = await Promise.all([
    User.findOne({ username: w }).lean(),
    User.findOne({ username: b }).lean(),
  ]);
  return {
    [w]: Math.max(0, Math.floor(Number(wa?.wallet) || 0)),
    [b]: Math.max(0, Math.floor(Number(ba?.wallet) || 0)),
  };
}

function syncServerBalancesFromSnapshot(st, balancesByUid) {
  const w = normalizeUid(st.whiteUid);
  const b = normalizeUid(st.blackUid);
  if (balancesByUid[w] !== undefined) serverBalancesByUid.set(w, balancesByUid[w]);
  if (balancesByUid[b] !== undefined) serverBalancesByUid.set(b, balancesByUid[b]);
}

async function getOrCreateUser(uidRaw) {
  const username = normalizeUid(uidRaw);
  if (!username) throw new Error("invalid uid");
  let doc = await User.findOne({ username });
  if (!doc) {
    doc = await User.create({
      username,
      wallet: 0,
      rating: 1000,
      wins: 0,
      losses: 0,
    });
    console.log("[mongo] user created", username);
  }
  serverBalancesByUid.set(username, Math.max(0, Math.floor(Number(doc.wallet) || 0)));
  return doc;
}

async function updateWallet(uidRaw, delta, meta = {}) {
  const username = normalizeUid(uidRaw);
  if (!username) return null;
  const d = Math.floor(Number(delta) || 0);
  const filter = d >= 0 ? { username } : { username, wallet: { $gte: -d } };
  const updated = await User.findOneAndUpdate(filter, { $inc: { wallet: d } }, { new: true });
  if (updated) {
    serverBalancesByUid.set(username, Math.max(0, Math.floor(Number(updated.wallet) || 0)));
    console.log("[mongo] wallet updated", username, updated.wallet, meta.reason || "");
  }
  return updated;
}

async function addWin(winnerUid, loserUid, session = null) {
  const w = normalizeUid(winnerUid);
  const l = normalizeUid(loserUid);
  const opts = session ? { session } : {};
  const wa = await User.findOne({ username: w }).session(session || null);
  const la = await User.findOne({ username: l }).session(session || null);
  if (!wa || !la) return;
  const expW = expectedScore(wa.rating, la.rating);
  const expL = expectedScore(la.rating, wa.rating);
  const newWR = Math.round(wa.rating + K_RATING * (1 - expW));
  const newLR = Math.round(la.rating + K_RATING * (0 - expL));
  await User.updateOne(
    { username: w },
    { $inc: { wins: 1 }, $set: { rating: Math.max(100, newWR) } },
    opts
  );
  await User.updateOne(
    { username: l },
    { $inc: { losses: 1 }, $set: { rating: Math.max(100, newLR) } },
    opts
  );
}

async function addLoss(loserUid, winnerUid, session = null) {
  await addWin(winnerUid, loserUid, session);
}

async function deductEntryFeesBoth(whiteUid, blackUid, betAmount) {
  const bet = Math.max(0, Math.floor(Number(betAmount) || 0));
  if (bet <= 0) return true;
  const w = normalizeUid(whiteUid);
  const b = normalizeUid(blackUid);
  const session = await mongoose.startSession();
  session.startTransaction();
  try {
    const uw = await User.findOneAndUpdate(
      { username: w, wallet: { $gte: bet } },
      { $inc: { wallet: -bet } },
      { new: true, session }
    );
    const ub = await User.findOneAndUpdate(
      { username: b, wallet: { $gte: bet } },
      { $inc: { wallet: -bet } },
      { new: true, session }
    );
    if (!uw || !ub) {
      await session.abortTransaction();
      return false;
    }
    await session.commitTransaction();
    serverBalancesByUid.set(w, Math.max(0, uw.wallet));
    serverBalancesByUid.set(b, Math.max(0, ub.wallet));
    console.log("[mongo] wallet updated", w, uw.wallet, "entry_deduct");
    console.log("[mongo] wallet updated", b, ub.wallet, "entry_deduct");
    return true;
  } catch (err) {
    await session.abortTransaction();
    console.error("[mongo] deductEntryFeesBoth", err);
    return false;
  } finally {
    session.endSession();
  }
}

async function applyGameResultToMongo(st, payload) {
  const gid = st.gameId;
  const paidMatch = st.mode === "paid" && st.betAmount > 0;
  const bet = Math.max(0, Math.floor(Number(st.betAmount) || 0));

  const session = await mongoose.startSession();
  try {
    await session.withTransaction(async () => {
      await GameSettlement.create([{ gameId: gid }], { session });

      if (!paidMatch) {
        if (!payload.draw && payload.winnerColor) {
          const winnerUid = payload.winnerColor === "white" ? st.whiteUid : st.blackUid;
          const loserUid = payload.winnerColor === "white" ? st.blackUid : st.whiteUid;
          await addWin(winnerUid, loserUid, session);
        }
        return;
      }

      if (payload.draw) {
        await User.updateOne(
          { username: normalizeUid(st.whiteUid) },
          { $inc: { wallet: bet } },
          { session }
        );
        await User.updateOne(
          { username: normalizeUid(st.blackUid) },
          { $inc: { wallet: bet } },
          { session }
        );
        return;
      }

      const winnerUid = payload.winnerColor === "white" ? st.whiteUid : st.blackUid;
      const loserUid = payload.winnerColor === "white" ? st.blackUid : st.whiteUid;
      const { winnerAmount } = calculateWinnerCreditServer(bet);
      await User.updateOne(
        { username: normalizeUid(winnerUid) },
        { $inc: { wallet: winnerAmount } },
        { session }
      );
      await addWin(winnerUid, loserUid, session);
    });
  } catch (err) {
    if (err && err.code === 11000) {
      const balancesByUid = await snapshotBalancesFromMongo(st.whiteUid, st.blackUid);
      syncServerBalancesFromSnapshot(st, balancesByUid);
      console.log("[match] duplicate settlement prevented", gid);
      return { balancesByUid, paid: paidMatch, alreadySettled: true };
    }
    console.error("[match] applyGameResultToMongo", err);
    throw err;
  } finally {
    session.endSession();
  }

  console.log("[match] ended", gid, payload.reason || "", !!payload.draw);
  if (paidMatch && !payload.draw && payload.winnerColor) {
    const { winnerAmount } = calculateWinnerCreditServer(bet);
    const wUid = payload.winnerColor === "white" ? st.whiteUid : st.blackUid;
    console.log("[match] payout completed", gid, normalizeUid(wUid), winnerAmount);
  }

  const balancesByUid = await snapshotBalancesFromMongo(st.whiteUid, st.blackUid);
  syncServerBalancesFromSnapshot(st, balancesByUid);
  return { balancesByUid, paid: paidMatch };
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
  if (!Number.isFinite(n) || n <= 0) {
    return null;
  }
  return n;
}

function isAdminRequestAuthorized(req) {
  if (!adminApiKey) {
    return false;
  }
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

app.get("/admin/live", requireAdmin, (_req, res) => {
  const sockets = [];
  for (const [, sock] of io.sockets.sockets) {
    const handshake = sock.handshake || {};
    sockets.push({
      socketId: sock.id,
      connected: sock.connected,
      verifiedUserId: sock.verifiedUserId || null,
      publicUid: typeof sock.publicUid === "string" ? sock.publicUid : "",
      room: typeof sock.room === "string" ? sock.room : sock.matchMeta?.room || null,
      matchGameId: sock.matchMeta?.gameId || null,
      matchColor: sock.matchMeta?.color || null,
      waitingQueueKey: sock.waitingQueueKey || null,
      firestoreWaiterKey: sock.firestoreMatchWaiterKey || null,
      ip: handshake.address || "",
      userAgent: String(handshake.headers?.["user-agent"] || "").slice(0, 220),
    });
  }

  const matches = [];
  for (const [room, st] of roomStates.entries()) {
    if (!st || st.isFinished) continue;
    let fen = "";
    try {
      fen = st.chess?.fen?.() || "";
    } catch {
      fen = "";
    }
    matches.push({
      room,
      gameId: st.gameId,
      mode: st.mode,
      betAmount: st.betAmount,
      whiteUid: st.whiteUid,
      blackUid: st.blackUid,
      toMove: st.toMove,
      fen,
      disconnectingColor: st.disconnectingColor,
      disconnectGraceEndsAt: st.disconnectGraceEndsAt,
    });
  }

  const queues = {};
  for (const [key, arr] of waitingQueues.entries()) {
    queues[key] = (arr || []).map((e) => ({
      uid: e.uid,
      socketId: e.socket?.id,
      mode: e.mode,
      betAmount: e.betAmount,
    }));
  }

  const firestoreWaiters = [];
  for (const [rid, entry] of firestoreMatchWaiters.entries()) {
    firestoreWaiters.push({
      firestoreMatchId: rid,
      uid: entry?.uid,
      socketId: entry?.socket?.id,
      connected: !!entry?.socket?.connected,
    });
  }

  res.json({
    ok: true,
    serverTime: Date.now(),
    mongo: mongoConnected() ? "connected" : "disconnected",
    sockets,
    matches,
    queues,
    firestoreWaiters,
  });
});

/** In-memory fallback when MongoDB is offline (paid games settle from memory banks). */
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

/** Remaining reconnect window (seconds); clients drive the AFK banner from this + 1 Hz refresh. */
function broadcastPlayerAfk(room, st) {
  if (!st.disconnectingColor || !st.disconnectGraceEndsAt) {
    return;
  }
  const sec = Math.max(
    0,
    Math.ceil((st.disconnectGraceEndsAt - Date.now()) / 1000),
  );
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

async function endMatch(room, payload) {
  const st = roomStates.get(room);
  if (!st || st.isFinished) {
    return;
  }
  st.isFinished = true;
  clearDisconnectGrace(st);

  let balancesByUid;
  let paid;
  let alreadySettled;

  if (mongoConnected()) {
    const r = await applyGameResultToMongo(st, payload);
    balancesByUid = r.balancesByUid;
    paid = r.paid;
    alreadySettled = r.alreadySettled;
  } else {
    const r = buildBalancesForGameResult(st, payload);
    balancesByUid = r.balancesByUid;
    paid = r.paid;
    alreadySettled = r.alreadySettled;
  }

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
    ...(alreadySettled ? { alreadySettled: true } : {}),
  };
  roomStates.delete(room);
  io.to(room).emit("gameResult", out);
}

/** Side to move ran out of time — opponent wins (blitz flag). */
function finalizeTimeoutFlag(room) {
  const st = roomStates.get(room);
  if (!st || st.isFinished) {
    return;
  }
  const now = Date.now();
  const bank = st.toMove === "w" ? st.whiteMs : st.blackMs;
  const elapsed = Math.max(0, now - st.turnClockStartedAt);
  if (bank - elapsed > 0) {
    return;
  }

  const winnerColor = st.toMove === "w" ? "black" : "white";
  void endMatch(room, {
    reason: "timeout",
    winnerColor,
    draw: false,
  }).catch((e) => console.error("[endMatch]", e));
}

/** 1 Hz: broadcast live clocks + detect flag fall (server-only time). */
function tickActiveGames() {
  for (const [room, st] of [...roomStates.entries()]) {
    if (!st || st.isFinished || !roomStates.has(room)) {
      continue;
    }
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
  if (!st || st.isFinished) {
    return;
  }
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
    void endMatch(room, {
      reason: "disconnect_forfeit",
      winnerColor,
      draw: false,
    }).catch((e) => console.error("[endMatch]", e));
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
  if (mode === "paid") {
    return `paid_${Number(betAmount) || 0}`;
  }
  return "free";
}

function moverCharFromMeta(meta) {
  if (!meta || (meta.color !== "white" && meta.color !== "black")) {
    return null;
  }
  return meta.color === "white" ? "w" : "b";
}

function handlePlayerResigned(socket, payload = {}) {
  const room = socket.matchMeta?.room;
  if (!room || socket.room !== room || !socket.matchMeta) {
    return;
  }
  if (socket.matchMeta.room !== room) {
    return;
  }
  const st = roomStates.get(room);
  if (!st || st.isFinished) {
    return;
  }
  const reqGid = payload.gameId != null ? String(payload.gameId) : "";
  const reqPid = payload.playerId != null ? String(payload.playerId) : "";
  if (reqGid && reqGid !== st.gameId) {
    return;
  }
  if (
    reqPid &&
    socket.matchMeta.playerId &&
    reqPid !== socket.matchMeta.playerId
  ) {
    return;
  }
  const loserColor = socket.matchMeta.color;
  const winnerColor = loserColor === "white" ? "black" : "white";
  void endMatch(room, {
    reason: "playerResigned",
    winnerColor,
    draw: false,
  }).catch((e) => console.error("[endMatch]", e));
}

function socketRequireJwtForPaid() {
  return (
    String(process.env.SOCKET_REQUIRE_JWT_FOR_PAID || "").toLowerCase() === "true" ||
    process.env.SOCKET_REQUIRE_JWT_FOR_PAID === "1"
  );
}

/**
 * Canonical `uid` string for matchmaking + Mongo wallet rows (`User.username` compatibility).
 * If handshake JWT is valid, ignores client `payload.uid` (anti-spoof).
 */
async function resolveJoinIdentity(socket, payload) {
  const mode = payload.mode === "paid" ? "paid" : "free";

  if (socket.verifiedUserId) {
    if (!mongoConnected()) {
      const err = new Error("Database offline — cannot verify login. Try again soon.");
      err.statusCode = 503;
      throw err;
    }
    const user = await User.findOne({ userId: socket.verifiedUserId, status: "active" }).lean();
    if (!user) {
      const err = new Error("Account not found or inactive.");
      err.statusCode = 401;
      throw err;
    }
    return {
      uid: normalizeUid(user.username),
      userData: {
        userId: user.userId,
        username: user.username,
        wallet: user.wallet,
        walletOnHold: user.walletOnHold || 0,
        rating: user.rating,
        wins: user.wins,
        losses: user.losses,
      },
    };
  }

  if (mode === "paid" && socketRequireJwtForPaid()) {
    const err = new Error("Login required for money matches.");
    err.statusCode = 401;
    throw err;
  }

  const raw =
    typeof payload.uid === "string" && payload.uid.length > 0 ? payload.uid : socket.id;
  const uid = normalizeUid(raw);
  if (!uid) {
    const err = new Error("Invalid player id.");
    err.statusCode = 400;
    throw err;
  }

  if (mode === "paid") {
    const guestLike =
      typeof payload.uid === "string" &&
      (payload.uid.startsWith("guest_") || payload.uid.startsWith("Guest_"));
    if (!payload.uid || (payload.uid === socket.id && !guestLike)) {
      const err = new Error(
        "Use a server-issued guest id for paid matches (POST /api/guest/issue), or log in."
      );
      err.statusCode = 400;
      throw err;
    }
  }

  if (mongoConnected()) {
    const doc = await getOrCreateUser(uid);
    return {
      uid: normalizeUid(doc.username),
      userData: {
        username: doc.username,
        wallet: doc.wallet,
        rating: doc.rating,
        wins: doc.wins,
        losses: doc.losses,
      },
    };
  }

  return {
    uid,
    userData: {
      username: uid,
      wallet: 0,
      rating: 1000,
      wins: 0,
      losses: 0,
    },
  };
}

io.on("connection", (socket) => {
  console.log("Player connected:", socket.id);
  socket.publicUid = "";

  socket.on("joinGame", async (payload = {}) => {
    let identity;
    try {
      identity = await resolveJoinIdentity(socket, payload);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Could not join.";
      socket.emit("matchmaking_error", { message: msg });
      return;
    }

    const { uid, userData } = identity;
    socket.userData = userData;
    socket.publicUid = uid;
    socket.emit("userData", userData);

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

      if (mode === "paid" && betAmount <= 0) {
        socket.emit("matchmaking_error", {
          message: "Invalid stake for a money match.",
        });
        return;
      }

      const room = `fs_${rid}`;

      if (!firestoreMatchWaiters.has(rid)) {
        if (mongoConnected()) {
          try {
            await getOrCreateUser(uid);
          } catch (err) {
            console.error("[joinGame]", err);
            socket.emit("matchmaking_error", { message: "Server error. Try again." });
            return;
          }
        }
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

      if (mongoConnected()) {
        try {
          await getOrCreateUser(whiteUid);
          await getOrCreateUser(blackUid);
          if (mode === "paid" && betAmount > 0) {
            const ok = await deductEntryFeesBoth(whiteUid, blackUid, betAmount);
            if (!ok) {
              first.socket.emit("matchmaking_error", {
                message: "Insufficient wallet balance.",
              });
              socket.emit("matchmaking_error", {
                message: "Insufficient wallet balance.",
              });
              return;
            }
          }
        } catch (err) {
          console.error("[joinGame]", err);
          first.socket.emit("matchmaking_error", { message: "Server error. Try again." });
          socket.emit("matchmaking_error", { message: "Server error. Try again." });
          return;
        }
      }

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

    if (mode === "paid" && betAmount <= 0) {
      socket.emit("matchmaking_error", {
        message: "Invalid stake for a money match.",
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

      if (mongoConnected()) {
        try {
          await getOrCreateUser(whiteUid);
          await getOrCreateUser(blackUid);
          if (mode === "paid" && betAmount > 0) {
            const ok = await deductEntryFeesBoth(whiteUid, blackUid, betAmount);
            if (!ok) {
              waitingPlayer.socket.emit("matchmaking_error", {
                message: "Insufficient wallet balance.",
              });
              socket.emit("matchmaking_error", {
                message: "Insufficient wallet balance.",
              });
              return;
            }
          }
        } catch (err) {
          console.error("[joinGame]", err);
          waitingPlayer.socket.emit("matchmaking_error", { message: "Server error. Try again." });
          socket.emit("matchmaking_error", { message: "Server error. Try again." });
          return;
        }
      }

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
      if (mongoConnected()) {
        try {
          await getOrCreateUser(uid);
        } catch (err) {
          console.error("[joinGame]", err);
          socket.emit("matchmaking_error", { message: "Server error. Try again." });
          return;
        }
      }
      queue.push(entry);
      socket.waitingQueueKey = key;
      socket.emit("waiting");
    }
  });

  socket.on("rejoinMatch", async (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : "";
    let uid =
      typeof payload.uid === "string" && payload.uid.length > 0
        ? payload.uid
        : "";
    if (!room || !uid) {
      socket.emit("rejoinFailed", { message: "Invalid rejoin payload." });
      return;
    }
    const st = roomStates.get(room);
    if (!st || st.isFinished) {
      socket.emit("rejoinFailed", { message: "Match not active." });
      return;
    }

    if (socket.verifiedUserId && mongoConnected()) {
      const user = await User.findOne({ userId: socket.verifiedUserId, status: "active" }).lean();
      if (!user) {
        socket.emit("rejoinFailed", { message: "Not authorized to rejoin this match." });
        return;
      }
      uid = user.username;
    }

    if (uid !== st.whiteUid && uid !== st.blackUid) {
      socket.emit("rejoinFailed", { message: "Not a player in this match." });
      return;
    }

    const color = uid === st.whiteUid ? "white" : "black";
    const returnedFromDisconnect = st.disconnectingColor === color;
    socket.join(room);
    socket.room = room;
    socket.publicUid = normalizeUid(uid);
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
      payload.player === "white" || payload.player === "black" ? payload.player : null;
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
    startDisconnectGrace(room, player);
  });

  socket.on("appForegrounded", (payload = {}) => {
    const room = typeof payload.room === "string" ? payload.room : "";
    const player =
      payload.player === "white" || payload.player === "black" ? payload.player : null;
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
    if (!room || socket.room !== room || !socket.matchMeta) {
      return;
    }
    const st = roomStates.get(room);
    if (!st || st.isFinished) {
      return;
    }
    const mover = moverCharFromMeta(socket.matchMeta);
    if (!mover || mover !== st.toMove) {
      return;
    }

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
    if (!moveTry) {
      return;
    }

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
      void endMatch(room, {
        reason: "checkmate",
        winnerColor,
        draw: false,
      }).catch((e) => console.error("[endMatch]", e));
      return;
    }
    if (st.chess.isDraw()) {
      void endMatch(room, {
        reason: "draw",
        draw: true,
        winnerColor: null,
      }).catch((e) => console.error("[endMatch]", e));
    }
  });

  socket.on("playerResigned", (payload) =>
    handlePlayerResigned(socket, payload)
  );
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
    if (!key) {
      return;
    }
    const queue = waitingQueues.get(key);
    if (!queue) {
      return;
    }
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

async function boot() {
  const uri = (process.env.MONGODB_URI || "").trim();
  if (uri) {
    try {
      await mongoose.connect(uri);
      console.log("[mongo] connected");
    } catch (err) {
      console.error("[mongo] connection failed — continuing without MongoDB:", err?.message || err);
    }
  } else {
    console.warn("[mongo] MONGODB_URI not set — running with in-memory wallets only");
  }

  server.listen(PORT, "0.0.0.0", () => {
    console.log(`Chess socket server listening on ${PORT}`);
  });
}

void boot().catch((err) => {
  console.error("[boot] fatal", err);
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`Chess socket server listening on ${PORT}`);
  });
});


### FILE: models/GameSettlement.js ###
const mongoose = require("mongoose");

const gameSettlementSchema = new mongoose.Schema({
  gameId: { type: String, required: true, unique: true, index: true },
  settledAt: { type: Date, default: Date.now },
});

module.exports =
  mongoose.models.GameSettlement || mongoose.model("GameSettlement", gameSettlementSchema);


### FILE: models/User.js ###
const crypto = require("crypto");
const mongoose = require("mongoose");

const accountStatuses = ["active", "suspended", "banned", "deleted"];

function makePublicCode(prefix) {
  return `${prefix}${crypto.randomBytes(4).toString("hex").toUpperCase()}`;
}

const userSchema = new mongoose.Schema(
  {
    // Existing socket server fields. Keep these stable for backwards compatibility.
    username: { type: String, required: true, unique: true, index: true, trim: true },
    wallet: { type: Number, default: 0, min: 0 },
    walletOnHold: { type: Number, default: 0, min: 0 },
    rating: { type: Number, default: 1000, min: 100 },
    wins: { type: Number, default: 0, min: 0 },
    losses: { type: Number, default: 0, min: 0 },

    // Production identity fields.
    userId: { type: String, unique: true, sparse: true, index: true },
    firebaseUid: { type: String, unique: true, sparse: true, index: true },
    phone: { type: String, unique: true, sparse: true, index: true },
    name: { type: String, trim: true, default: "" },
    email: { type: String, lowercase: true, trim: true, default: "" },
    avatarUrl: { type: String, trim: true, default: "" },

    roles: {
      type: [String],
      enum: ["user", "admin", "super_admin"],
      default: ["user"],
      index: true,
    },
    status: {
      type: String,
      enum: accountStatuses,
      default: "active",
      index: true,
    },
    statusReason: { type: String, trim: true, default: "" },
    statusUpdatedAt: { type: Date, default: null },

    referralCode: { type: String, unique: true, sparse: true, index: true },
    referredBy: { type: String, default: null, index: true },

    walletStats: {
      totalDeposits: { type: Number, default: 0, min: 0 },
      totalWithdrawals: { type: Number, default: 0, min: 0 },
      totalWinnings: { type: Number, default: 0, min: 0 },
      totalLosses: { type: Number, default: 0, min: 0 },
      totalCommissionPaid: { type: Number, default: 0, min: 0 },
    },

    matchStats: {
      totalMatches: { type: Number, default: 0, min: 0 },
      paidMatches: { type: Number, default: 0, min: 0 },
      friendlyMatches: { type: Number, default: 0, min: 0 },
      draws: { type: Number, default: 0, min: 0 },
    },

    lastLoginAt: { type: Date, default: null },
    registeredAt: { type: Date, default: Date.now, index: true },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { timestamps: true }
);

userSchema.pre("validate", function assignPublicIds(next) {
  if (!this.userId) this.userId = makePublicCode("USR");
  if (!this.referralCode) this.referralCode = makePublicCode("REF");
  next();
});

module.exports = mongoose.models.User || mongoose.model("User", userSchema);


### FILE: models/OtpChallenge.js ###
const mongoose = require("mongoose");

const otpChallengeSchema = new mongoose.Schema(
  {
    challengeId: { type: String, required: true, unique: true, index: true },
    phone: { type: String, required: true, index: true },
    otpHash: { type: String, required: true },
    purpose: { type: String, enum: ["login"], default: "login", index: true },
    status: {
      type: String,
      enum: ["pending", "verified", "expired", "failed"],
      default: "pending",
      index: true,
    },
    attempts: { type: Number, default: 0, min: 0 },
    maxAttempts: { type: Number, default: 5, min: 1 },
    expiresAt: { type: Date, required: true },
    verifiedAt: { type: Date, default: null },
    ip: { type: String, default: "" },
    userAgent: { type: String, default: "" },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { timestamps: true }
);

otpChallengeSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 3600 });
otpChallengeSchema.index({ phone: 1, status: 1, createdAt: -1 });

module.exports =
  mongoose.models.OtpChallenge || mongoose.model("OtpChallenge", otpChallengeSchema);


### FILE: models/Match.js ###
const mongoose = require("mongoose");

const participantSchema = new mongoose.Schema(
  {
    user: { type: mongoose.Schema.Types.ObjectId, ref: "User", default: null },
    userId: { type: String, required: true, index: true },
    username: { type: String, required: true },
    color: { type: String, enum: ["white", "black"], required: true },
  },
  { _id: false }
);

const matchSchema = new mongoose.Schema(
  {
    matchId: { type: String, required: true, unique: true, index: true },
    socketGameId: { type: String, default: null, index: true },
    mode: { type: String, enum: ["friendly", "paid"], required: true, index: true },
    status: {
      type: String,
      enum: ["requested", "accepted", "active", "completed", "cancelled", "disputed"],
      default: "requested",
      index: true,
    },
    stakeAmount: { type: Number, default: 0, min: 0 },
    totalPot: { type: Number, default: 0, min: 0 },
    commissionAmount: { type: Number, default: 0, min: 0 },
    winnerPayout: { type: Number, default: 0, min: 0 },
    participants: { type: [participantSchema], validate: (v) => v.length <= 2 },
    winnerUserId: { type: String, default: null, index: true },
    draw: { type: Boolean, default: false },
    endReason: { type: String, default: "" },
    startedAt: { type: Date, default: null },
    completedAt: { type: Date, default: null, index: true },
    pgn: { type: String, default: "" },
    finalFen: { type: String, default: "" },
    moves: { type: [mongoose.Schema.Types.Mixed], default: [] },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { timestamps: true }
);

matchSchema.index({ "participants.userId": 1, createdAt: -1 });

module.exports = mongoose.models.Match || mongoose.model("Match", matchSchema);


### FILE: models/MatchRequest.js ###
const mongoose = require("mongoose");

const matchRequestSchema = new mongoose.Schema(
  {
    requestId: { type: String, required: true, unique: true, index: true },
    fromUser: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true, index: true },
    fromUserId: { type: String, required: true, index: true },
    toUser: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true, index: true },
    toUserId: { type: String, required: true, index: true },
    mode: { type: String, enum: ["friendly", "paid"], required: true, index: true },
    stakeAmount: { type: Number, default: 0, min: 0 },
    status: {
      type: String,
      enum: ["pending", "accepted", "rejected", "cancelled", "expired"],
      default: "pending",
      index: true,
    },
    expiresAt: { type: Date, required: true, index: true },
    respondedAt: { type: Date, default: null },
    matchId: { type: String, default: null, index: true },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { timestamps: true }
);

matchRequestSchema.index({ toUserId: 1, status: 1, createdAt: -1 });
matchRequestSchema.index({ fromUserId: 1, status: 1, createdAt: -1 });

module.exports =
  mongoose.models.MatchRequest || mongoose.model("MatchRequest", matchRequestSchema);


### FILE: models/Transaction.js ###
const mongoose = require("mongoose");

const transactionSchema = new mongoose.Schema(
  {
    transactionId: { type: String, required: true, unique: true, index: true },
    user: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true, index: true },
    userId: { type: String, required: true, index: true },
    kind: { type: String, enum: ["deposit", "withdrawal"], required: true, index: true },
    status: {
      type: String,
      enum: ["pending", "approved", "rejected", "cancelled", "processing"],
      default: "pending",
      index: true,
    },
    amount: { type: Number, required: true, min: 1 },
    currency: { type: String, default: "INR" },
    utr: { type: String, trim: true, default: "", index: true },
    screenshotUrl: { type: String, trim: true, default: "" },
    paymentMethod: { type: String, trim: true, default: "upi" },
    bankAccount: {
      accountHolder: { type: String, trim: true, default: "" },
      accountNumberMasked: { type: String, trim: true, default: "" },
      ifsc: { type: String, trim: true, default: "" },
      upiId: { type: String, trim: true, default: "" },
    },
    reviewedBy: { type: String, default: null, index: true },
    reviewedAt: { type: Date, default: null },
    rejectionReason: { type: String, trim: true, default: "" },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { timestamps: true }
);

module.exports =
  mongoose.models.Transaction || mongoose.model("Transaction", transactionSchema);


### FILE: models/WalletLedger.js ###
const mongoose = require("mongoose");

const walletLedgerSchema = new mongoose.Schema(
  {
    user: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true, index: true },
    userId: { type: String, required: true, index: true },
    type: {
      type: String,
      enum: ["credit", "debit", "hold", "release", "settlement", "refund", "adjustment"],
      required: true,
      index: true,
    },
    reason: {
      type: String,
      enum: [
        "deposit_approved",
        "withdrawal_approved",
        "match_entry_hold",
        "match_entry_release",
        "match_win_payout",
        "match_draw_refund",
        "admin_credit",
        "admin_debit",
        "admin_set_balance",
        "commission",
        "fraud_reversal",
      ],
      required: true,
      index: true,
    },
    amount: { type: Number, required: true, min: 0 },
    balanceBefore: { type: Number, required: true, min: 0 },
    balanceAfter: { type: Number, required: true, min: 0 },
    holdBefore: { type: Number, default: 0, min: 0 },
    holdAfter: { type: Number, default: 0, min: 0 },
    matchId: { type: String, default: null, index: true },
    transactionId: { type: String, default: null, index: true },
    idempotencyKey: { type: String, required: true, unique: true, index: true },
    actorUserId: { type: String, default: null, index: true },
    actorRole: { type: String, default: "system" },
    note: { type: String, default: "" },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { timestamps: true }
);

module.exports =
  mongoose.models.WalletLedger || mongoose.model("WalletLedger", walletLedgerSchema);


### FILE: models/AdminAuditLog.js ###
const mongoose = require("mongoose");

const adminAuditLogSchema = new mongoose.Schema(
  {
    adminUserId: { type: String, required: true, index: true },
    action: { type: String, required: true, index: true },
    targetType: {
      type: String,
      enum: ["user", "wallet", "transaction", "match", "system"],
      required: true,
      index: true,
    },
    targetId: { type: String, default: null, index: true },
    ip: { type: String, default: "" },
    userAgent: { type: String, default: "" },
    before: { type: mongoose.Schema.Types.Mixed, default: null },
    after: { type: mongoose.Schema.Types.Mixed, default: null },
    metadata: { type: mongoose.Schema.Types.Mixed, default: {} },
  },
  { timestamps: true }
);

module.exports =
  mongoose.models.AdminAuditLog || mongoose.model("AdminAuditLog", adminAuditLogSchema);


### FILE: src/config/env.js ###
function readString(name, fallback = "") {
  return (process.env[name] || fallback).trim();
}

function readNumber(name, fallback) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) ? value : fallback;
}

module.exports = {
  nodeEnv: readString("NODE_ENV", "development"),
  jwtSecret: readString("JWT_SECRET"),
  jwtExpiresIn: readString("JWT_EXPIRES_IN", "7d"),
  otpHashSecret: readString("OTP_HASH_SECRET"),
  otpTtlSeconds: readNumber("OTP_TTL_SECONDS", 300),
  adminApiKey: readString("ADMIN_API_KEY"),
  corsOrigin: readString("CORS_ORIGIN", "*"),
  smsProvider: readString("SMS_PROVIDER", "console"),
  smsHttpUrl: readString("SMS_HTTP_URL"),
  smsHttpAuthHeader: readString("SMS_HTTP_AUTH_HEADER"),
  smsHttpBodyTemplate: readString("SMS_HTTP_BODY_TEMPLATE"),
  authRateLimitWindowMs: readNumber("AUTH_RATE_LIMIT_WINDOW_MS", 15 * 60 * 1000),
  authRateLimitMax: readNumber("AUTH_RATE_LIMIT_MAX", 30),
  apiRateLimitWindowMs: readNumber("API_RATE_LIMIT_WINDOW_MS", 60 * 1000),
  apiRateLimitMax: readNumber("API_RATE_LIMIT_MAX", 120),
};


### FILE: src/middleware/rateLimits.js ###
const rateLimit = require("express-rate-limit");
const env = require("../config/env");

const standardHandler = (_req, res) => {
  res.status(429).json({
    ok: false,
    error: "Too many requests. Please try again later.",
  });
};

const apiLimiter = rateLimit({
  windowMs: env.apiRateLimitWindowMs,
  max: env.apiRateLimitMax,
  standardHeaders: true,
  legacyHeaders: false,
  handler: standardHandler,
});

const authLimiter = rateLimit({
  windowMs: env.authRateLimitWindowMs,
  max: env.authRateLimitMax,
  standardHeaders: true,
  legacyHeaders: false,
  handler: standardHandler,
});

module.exports = { apiLimiter, authLimiter };


### FILE: src/middleware/validate.js ###
function validate(schema, source = "body") {
  return (req, res, next) => {
    const parsed = schema.safeParse(req[source] || {});
    if (!parsed.success) {
      res.status(400).json({
        ok: false,
        error: "Validation failed.",
        details: parsed.error.flatten(),
      });
      return;
    }

    req[source] = parsed.data;
    next();
  };
}

module.exports = { validate };


### FILE: src/middleware/auth.js ###
const jwt = require("jsonwebtoken");
const User = require("../../models/User");
const env = require("../config/env");

function getBearerToken(req) {
  const header = req.get("authorization") || "";
  if (!header.startsWith("Bearer ")) return "";
  return header.slice(7).trim();
}

function getCookie(req, name) {
  const raw = req.get("cookie") || "";
  const pairs = raw.split(";").map((part) => part.trim());
  for (const pair of pairs) {
    const separator = pair.indexOf("=");
    if (separator <= 0) continue;
    const key = pair.slice(0, separator);
    if (key === name) return decodeURIComponent(pair.slice(separator + 1));
  }
  return "";
}

async function requireAuth(req, res, next) {
  const token = getBearerToken(req);
  if (!token) {
    res.status(401).json({ ok: false, error: "Missing bearer token." });
    return;
  }

  if (!env.jwtSecret) {
    res.status(503).json({ ok: false, error: "JWT_SECRET is not configured." });
    return;
  }

  try {
    const payload = jwt.verify(token, env.jwtSecret);
    const user = await User.findOne({ userId: payload.sub }).lean();
    if (!user || user.status !== "active") {
      res.status(401).json({ ok: false, error: "Account is not active." });
      return;
    }

    req.auth = {
      userId: user.userId,
      firebaseUid: user.firebaseUid,
      roles: user.roles || ["user"],
      phone: user.phone || "",
    };
    req.user = user;
    next();
  } catch (_error) {
    res.status(401).json({ ok: false, error: "Invalid or expired token." });
  }
}

function hasAdminApiKey(req) {
  if (!env.adminApiKey) return false;
  const headerKey = (req.get("x-admin-key") || "").trim();
  if (headerKey && headerKey === env.adminApiKey) return true;

  const token = getBearerToken(req);
  return token === env.adminApiKey;
}

function getAdminCookieAuth(req) {
  if (!env.jwtSecret) return null;
  const token = getCookie(req, "admin_session");
  if (!token) return null;

  try {
    const payload = jwt.verify(token, env.jwtSecret, {
      issuer: "chess-bet-app-admin",
    });
    const roles = Array.isArray(payload.roles) ? payload.roles : [];
    if (!roles.includes("admin") && !roles.includes("super_admin")) return null;

    return {
      userId: payload.sub || "admin_session",
      roles,
      adminCookie: true,
    };
  } catch (_error) {
    return null;
  }
}

async function requireAdmin(req, res, next) {
  if (hasAdminApiKey(req)) {
    req.auth = {
      userId: "legacy_admin_key",
      roles: ["admin"],
      legacyAdminKey: true,
    };
    next();
    return;
  }

  const cookieAuth = getAdminCookieAuth(req);
  if (cookieAuth) {
    req.auth = cookieAuth;
    next();
    return;
  }

  await requireAuth(req, res, () => {
    const roles = req.auth?.roles || [];
    if (!roles.includes("admin") && !roles.includes("super_admin")) {
      res.status(403).json({ ok: false, error: "Admin access required." });
      return;
    }
    next();
  });
}

module.exports = { getCookie, requireAuth, requireAdmin };


### FILE: src/services/authService.js ###
const crypto = require("crypto");
const jwt = require("jsonwebtoken");
const User = require("../../models/User");
const env = require("../config/env");

function normalizePhone(phone) {
  if (!phone) return "";
  const text = String(phone).trim();
  if (!text) return "";
  return text.startsWith("+") ? text : `+${text.replace(/\D/g, "")}`;
}

function normalizeUsername(username) {
  return String(username || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .slice(0, 24);
}

function makeUsernameFromPhone(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  const suffix = digits.slice(-4) || crypto.randomBytes(2).toString("hex");
  return `player_${suffix}_${crypto.randomBytes(2).toString("hex")}`;
}

async function reserveUsername(preferred, phone) {
  const base = normalizeUsername(preferred) || makeUsernameFromPhone(phone);
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const candidate = attempt === 0 ? base : `${base}_${crypto.randomBytes(2).toString("hex")}`;
    const exists = await User.exists({ username: candidate });
    if (!exists) return candidate;
  }
  return `player_${crypto.randomBytes(6).toString("hex")}`;
}

function publicUser(user) {
  return {
    userId: user.userId,
    firebaseUid: user.firebaseUid,
    name: user.name,
    phone: user.phone,
    username: user.username,
    wallet: user.wallet,
    walletOnHold: user.walletOnHold || 0,
    rating: user.rating,
    wins: user.wins,
    losses: user.losses,
    referralCode: user.referralCode,
    roles: user.roles,
    status: user.status,
    registeredAt: user.registeredAt,
    walletStats: user.walletStats,
    matchStats: user.matchStats,
  };
}

function issueJwt(user) {
  if (!env.jwtSecret) {
    throw new Error("JWT_SECRET is not configured.");
  }

  return jwt.sign(
    {
      roles: user.roles || ["user"],
      firebaseUid: user.firebaseUid,
      phone: user.phone,
    },
    env.jwtSecret,
    {
      subject: user.userId,
      expiresIn: env.jwtExpiresIn,
      issuer: "chess-bet-app",
    }
  );
}

async function signInWithVerifiedPhone({ phone, name, username, referralCode }) {
  const normalizedPhone = normalizePhone(phone);
  let user = await User.findOne({ phone: normalizedPhone });

  if (!user) {
    const finalUsername = await reserveUsername(username, normalizedPhone);
    user = await User.create({
      phone: normalizedPhone,
      name: String(name || "").trim(),
      username: finalUsername,
      referredBy: referralCode || null,
      lastLoginAt: new Date(),
    });
  } else {
    if (name && !user.name) user.name = String(name).trim();
    user.lastLoginAt = new Date();
    await user.save();
  }

  const token = issueJwt(user);
  return { token, user: publicUser(user) };
}

module.exports = {
  issueJwt,
  normalizePhone,
  publicUser,
  signInWithVerifiedPhone,
};


### FILE: src/services/otpService.js ###
const crypto = require("crypto");
const OtpChallenge = require("../../models/OtpChallenge");
const env = require("../config/env");
const { normalizePhone, signInWithVerifiedPhone } = require("./authService");
const { sendOtpSms } = require("./smsService");

function otpSecret() {
  const secret = env.otpHashSecret || env.jwtSecret;
  if (!secret) {
    const error = new Error("OTP_HASH_SECRET or JWT_SECRET is not configured.");
    error.statusCode = 503;
    throw error;
  }
  return secret;
}

function makeOtp() {
  return String(crypto.randomInt(100000, 1000000));
}

function hashOtp({ challengeId, phone, otp }) {
  return crypto
    .createHmac("sha256", otpSecret())
    .update(`${challengeId}:${phone}:${otp}`)
    .digest("hex");
}

async function requestOtp({ phone, ip, userAgent }) {
  const normalizedPhone = normalizePhone(phone);
  if (!/^\+\d{8,15}$/.test(normalizedPhone)) {
    const error = new Error("Enter a valid phone number with country code.");
    error.statusCode = 400;
    throw error;
  }

  const recentCount = await OtpChallenge.countDocuments({
    phone: normalizedPhone,
    createdAt: { $gte: new Date(Date.now() - 15 * 60 * 1000) },
  });

  if (recentCount >= 5) {
    const error = new Error("Too many OTP requests. Please try again later.");
    error.statusCode = 429;
    throw error;
  }

  const challengeId = `otp_${crypto.randomUUID()}`;
  const otp = makeOtp();
  const otpHash = hashOtp({ challengeId, phone: normalizedPhone, otp });
  const expiresAt = new Date(Date.now() + env.otpTtlSeconds * 1000);

  await OtpChallenge.updateMany(
    { phone: normalizedPhone, status: "pending" },
    { $set: { status: "expired" } }
  );

  await OtpChallenge.create({
    challengeId,
    phone: normalizedPhone,
    otpHash,
    expiresAt,
    ip,
    userAgent,
  });

  let smsResult;
  try {
    smsResult = await sendOtpSms({ phone: normalizedPhone, otp });
  } catch (error) {
    await OtpChallenge.updateOne({ challengeId }, { $set: { status: "failed" } });
    throw error;
  }

  return {
    challengeId,
    phone: normalizedPhone,
    expiresInSeconds: env.otpTtlSeconds,
    ...(smsResult.devOtp ? { devOtp: smsResult.devOtp } : {}),
  };
}

async function verifyOtp({ challengeId, phone, otp, name, username, referralCode }) {
  const normalizedPhone = normalizePhone(phone);
  const code = String(otp || "").trim();
  if (!challengeId || !/^\d{6}$/.test(code)) {
    const error = new Error("Enter a valid OTP.");
    error.statusCode = 400;
    throw error;
  }

  const challenge = await OtpChallenge.findOne({ challengeId, phone: normalizedPhone });
  if (!challenge || challenge.status !== "pending") {
    const error = new Error("OTP request not found or already used.");
    error.statusCode = 400;
    throw error;
  }

  if (challenge.expiresAt.getTime() < Date.now()) {
    challenge.status = "expired";
    await challenge.save();
    const error = new Error("OTP has expired. Please request a new code.");
    error.statusCode = 400;
    throw error;
  }

  const expected = hashOtp({ challengeId, phone: normalizedPhone, otp: code });
  if (expected !== challenge.otpHash) {
    challenge.attempts += 1;
    if (challenge.attempts >= challenge.maxAttempts) {
      challenge.status = "failed";
    }
    await challenge.save();
    const error = new Error("Incorrect OTP.");
    error.statusCode = 400;
    throw error;
  }

  challenge.status = "verified";
  challenge.verifiedAt = new Date();
  await challenge.save();

  return signInWithVerifiedPhone({
    phone: normalizedPhone,
    name,
    username,
    referralCode,
  });
}

module.exports = { requestOtp, verifyOtp };


### FILE: src/services/smsService.js ###
const env = require("../config/env");

function renderTemplate(template, values) {
  return Object.entries(values).reduce(
    (out, [key, value]) => out.replaceAll(`{{${key}}}`, String(value)),
    template
  );
}

function maskPhone(phone) {
  return `${phone.slice(0, 3)}****${phone.slice(-3)}`;
}

async function sendGenericSms({ phone, message, otp }) {
  if (!env.smsHttpUrl || !env.smsHttpBodyTemplate) {
    throw new Error("SMS_HTTP_URL and SMS_HTTP_BODY_TEMPLATE are required.");
  }

  const headers = { "content-type": "application/json" };
  if (env.smsHttpAuthHeader) {
    const separator = env.smsHttpAuthHeader.indexOf(":");
    if (separator > 0) {
      headers[env.smsHttpAuthHeader.slice(0, separator).trim()] =
        env.smsHttpAuthHeader.slice(separator + 1).trim();
    }
  }

  const body = renderTemplate(env.smsHttpBodyTemplate, { phone, message, otp });
  const response = await fetch(env.smsHttpUrl, {
    method: "POST",
    headers,
    body,
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`SMS provider failed with ${response.status}: ${text.slice(0, 200)}`);
  }
}

async function sendOtpSms({ phone, otp }) {
  const message = `Your ChessApp verification code is ${otp}. It expires in 5 minutes.`;
  const provider = env.smsProvider.toLowerCase();

  if (provider === "generic") {
    await sendGenericSms({ phone, message, otp });
    return { provider };
  }

  /**
   * `SMS_PROVIDER=console` (default): log OTP and return `devOtp` for the app (no real SMS).
   * Works in production too — fine for VPS testing. For real SMS set SMS_PROVIDER=generic
   * and SMS_HTTP_URL / SMS_HTTP_BODY_TEMPLATE (or another provider you add here).
   */
  if (provider === "console") {
    console.log(`[otp] ${maskPhone(phone)} code=${otp}`);
    return { provider, devOtp: otp };
  }

  throw new Error("Production SMS provider is not configured.");
}

module.exports = { sendOtpSms };


### FILE: src/services/walletService.js ###
const crypto = require("crypto");
const mongoose = require("mongoose");
const User = require("../../models/User");
const Transaction = require("../../models/Transaction");
const WalletLedger = require("../../models/WalletLedger");

function makeId(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function money(value) {
  const amount = Math.floor(Number(value));
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return amount;
}

async function writeLedger({
  session,
  user,
  type,
  reason,
  amount,
  balanceBefore,
  balanceAfter,
  holdBefore,
  holdAfter,
  matchId = null,
  transactionId = null,
  idempotencyKey,
  actorUserId = null,
  actorRole = "system",
  note = "",
  metadata = {},
}) {
  const [entry] = await WalletLedger.create(
    [
      {
        user: user._id,
        userId: user.userId,
        type,
        reason,
        amount,
        balanceBefore,
        balanceAfter,
        holdBefore,
        holdAfter,
        matchId,
        transactionId,
        idempotencyKey,
        actorUserId,
        actorRole,
        note,
        metadata,
      },
    ],
    { session }
  );
  return entry;
}

async function creditUser({ userId, amount, reason, actorUserId, transactionId, note, metadata }) {
  const creditAmount = money(amount);
  if (!creditAmount) throw new Error("Amount must be positive.");

  const session = await mongoose.startSession();
  try {
    let updatedUser;
    await session.withTransaction(async () => {
      const user = await User.findOne({ userId }).session(session);
      if (!user) throw new Error("User not found.");

      const balanceBefore = user.wallet;
      user.wallet += creditAmount;
      if (reason === "deposit_approved") {
        user.walletStats.totalDeposits += creditAmount;
      }
      await user.save({ session });
      updatedUser = user;

      await writeLedger({
        session,
        user,
        type: "credit",
        reason,
        amount: creditAmount,
        balanceBefore,
        balanceAfter: user.wallet,
        holdBefore: user.walletOnHold || 0,
        holdAfter: user.walletOnHold || 0,
        transactionId,
        actorUserId,
        actorRole: actorUserId ? "admin" : "system",
        note,
        metadata,
        idempotencyKey: transactionId
          ? `wallet:${reason}:${transactionId}`
          : `wallet:${reason}:${userId}:${makeId("idem")}`,
      });
    });
    return updatedUser;
  } finally {
    session.endSession();
  }
}

async function debitUser({ userId, amount, reason, actorUserId, transactionId, note, metadata }) {
  const debitAmount = money(amount);
  if (!debitAmount) throw new Error("Amount must be positive.");

  const session = await mongoose.startSession();
  try {
    let updatedUser;
    await session.withTransaction(async () => {
      const user = await User.findOne({ userId, wallet: { $gte: debitAmount } }).session(session);
      if (!user) throw new Error("Insufficient wallet balance.");

      const balanceBefore = user.wallet;
      user.wallet -= debitAmount;
      if (reason === "withdrawal_approved") {
        user.walletStats.totalWithdrawals += debitAmount;
      }
      await user.save({ session });
      updatedUser = user;

      await writeLedger({
        session,
        user,
        type: "debit",
        reason,
        amount: debitAmount,
        balanceBefore,
        balanceAfter: user.wallet,
        holdBefore: user.walletOnHold || 0,
        holdAfter: user.walletOnHold || 0,
        transactionId,
        actorUserId,
        actorRole: actorUserId ? "admin" : "system",
        note,
        metadata,
        idempotencyKey: transactionId
          ? `wallet:${reason}:${transactionId}`
          : `wallet:${reason}:${userId}:${makeId("idem")}`,
      });
    });
    return updatedUser;
  } finally {
    session.endSession();
  }
}

async function createPaymentRequest({ user, kind, amount, utr, screenshotUrl, bankAccount }) {
  const requestAmount = money(amount);
  if (!requestAmount) throw new Error("Amount must be positive.");

  return Transaction.create({
    transactionId: makeId(kind === "deposit" ? "dep" : "wd"),
    user: user._id,
    userId: user.userId,
    kind,
    amount: requestAmount,
    utr,
    screenshotUrl,
    bankAccount,
  });
}

async function reviewTransaction({ transactionId, status, adminUserId, rejectionReason }) {
  const transaction = await Transaction.findOne({ transactionId });
  if (!transaction) throw new Error("Transaction not found.");
  if (transaction.status !== "pending") throw new Error("Transaction is already reviewed.");

  if (status === "rejected") {
    transaction.status = "rejected";
    transaction.reviewedBy = adminUserId;
    transaction.reviewedAt = new Date();
    transaction.rejectionReason = rejectionReason || "";
    await transaction.save();
    return transaction;
  }

  if (transaction.kind === "deposit") {
    await creditUser({
      userId: transaction.userId,
      amount: transaction.amount,
      reason: "deposit_approved",
      actorUserId: adminUserId,
      transactionId: transaction.transactionId,
    });
  } else {
    await debitUser({
      userId: transaction.userId,
      amount: transaction.amount,
      reason: "withdrawal_approved",
      actorUserId: adminUserId,
      transactionId: transaction.transactionId,
    });
  }

  transaction.status = "approved";
  transaction.reviewedBy = adminUserId;
  transaction.reviewedAt = new Date();
  await transaction.save();
  return transaction;
}

module.exports = {
  createPaymentRequest,
  creditUser,
  debitUser,
  reviewTransaction,
};


### FILE: src/routes/index.js ###
const express = require("express");
const helmet = require("helmet");
const { apiLimiter } = require("../middleware/rateLimits");
const authRoutes = require("./auth.routes");
const walletRoutes = require("./wallet.routes");
const matchRoutes = require("./match.routes");
const adminRoutes = require("./admin.routes");
const guestRoutes = require("./guest.routes");

function registerApiRoutes(app) {
  const router = express.Router();

  router.use(helmet());
  router.use(apiLimiter);

  router.get("/health", (_req, res) => {
    res.json({ ok: true, service: "chess-api" });
  });
  router.use("/guest", guestRoutes);
  router.use("/auth", authRoutes);
  router.use("/wallet", walletRoutes);
  router.use("/matches", matchRoutes);
  router.use("/admin", adminRoutes);

  app.use("/api", router);
}

module.exports = { registerApiRoutes };


### FILE: src/routes/guest.routes.js ###
const express = require("express");
const crypto = require("crypto");
const mongoose = require("mongoose");
const User = require("../../models/User");
const { authLimiter } = require("../middleware/rateLimits");

const router = express.Router();

/**
 * Mint a unique guest username for Socket.IO `joinGame` / paid queue.
 * Client should persist `guestId` and send it as `uid` until the user logs in (JWT on socket).
 */
router.post("/issue", authLimiter, async (_req, res) => {
  try {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const guestId = `guest_${crypto.randomInt(10000, 100000)}`;
      if (mongoose.connection.readyState !== 1) {
        res.json({ ok: true, guestId });
        return;
      }
      const exists = await User.exists({ username: guestId });
      if (!exists) {
        res.json({ ok: true, guestId });
        return;
      }
    }
    const guestId = `guest_${crypto.randomBytes(6).toString("hex")}`;
    res.json({ ok: true, guestId });
  } catch (err) {
    console.error("[guest/issue]", err);
    res.status(500).json({ ok: false, error: "Could not mint guest id." });
  }
});

module.exports = router;


### FILE: src/routes/auth.routes.js ###
const express = require("express");
const { z } = require("zod");
const { validate } = require("../middleware/validate");
const { authLimiter } = require("../middleware/rateLimits");
const { requireAuth } = require("../middleware/auth");
const { publicUser } = require("../services/authService");
const { requestOtp, verifyOtp } = require("../services/otpService");

const router = express.Router();

const requestOtpSchema = z.object({
  phone: z.string().trim().min(8).max(16),
});

const verifyOtpSchema = z.object({
  challengeId: z.string().trim().min(10),
  phone: z.string().trim().min(8).max(16),
  otp: z.string().trim().regex(/^\d{6}$/),
  name: z.string().trim().max(80).optional().default(""),
  username: z.string().trim().min(3).max(24).optional().default(""),
  referralCode: z.string().trim().max(32).optional().default(""),
});

router.post(
  "/otp/request",
  authLimiter,
  validate(requestOtpSchema),
  async (req, res) => {
    try {
      const result = await requestOtp({
        phone: req.body.phone,
        ip: req.ip,
        userAgent: req.get("user-agent") || "",
      });
      res.json({ ok: true, ...result });
    } catch (error) {
      res.status(error.statusCode || 400).json({
        ok: false,
        error: error.message || "Could not send OTP.",
      });
    }
  }
);

router.post(
  "/otp/verify",
  authLimiter,
  validate(verifyOtpSchema),
  async (req, res) => {
    try {
      const result = await verifyOtp(req.body);
      res.json({ ok: true, ...result });
    } catch (error) {
      res.status(error.statusCode || 400).json({
        ok: false,
        error: error.message || "Could not verify OTP.",
      });
    }
  }
);

router.get("/me", requireAuth, (req, res) => {
  res.json({ ok: true, user: publicUser(req.user) });
});

module.exports = router;


### FILE: src/routes/wallet.routes.js ###
const express = require("express");
const { z } = require("zod");
const Transaction = require("../../models/Transaction");
const WalletLedger = require("../../models/WalletLedger");
const { requireAuth } = require("../middleware/auth");
const { validate } = require("../middleware/validate");
const { createPaymentRequest } = require("../services/walletService");

const router = express.Router();

const depositSchema = z.object({
  amount: z.number().int().positive(),
  utr: z.string().trim().min(4).max(80),
  screenshotUrl: z.string().trim().url().optional().default(""),
});

const withdrawalSchema = z.object({
  amount: z.number().int().positive(),
  bankAccount: z
    .object({
      accountHolder: z.string().trim().max(100).optional().default(""),
      accountNumberMasked: z.string().trim().max(32).optional().default(""),
      ifsc: z.string().trim().max(20).optional().default(""),
      upiId: z.string().trim().max(80).optional().default(""),
    })
    .optional()
    .default({}),
});

router.get("/", requireAuth, async (req, res) => {
  res.json({
    ok: true,
    wallet: {
      balance: req.user.wallet,
      onHold: req.user.walletOnHold || 0,
      stats: req.user.walletStats,
    },
  });
});

router.get("/history", requireAuth, async (req, res) => {
  const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 50));
  const rows = await WalletLedger.find({ userId: req.auth.userId })
    .sort({ createdAt: -1 })
    .limit(limit)
    .lean();
  res.json({ ok: true, transactions: rows });
});

router.post("/deposit-requests", requireAuth, validate(depositSchema), async (req, res) => {
  try {
    const transaction = await createPaymentRequest({
      user: req.user,
      kind: "deposit",
      amount: req.body.amount,
      utr: req.body.utr,
      screenshotUrl: req.body.screenshotUrl,
    });
    res.status(201).json({ ok: true, transaction });
  } catch (error) {
    res.status(400).json({ ok: false, error: error.message });
  }
});

router.post(
  "/withdrawal-requests",
  requireAuth,
  validate(withdrawalSchema),
  async (req, res) => {
    try {
      const transaction = await createPaymentRequest({
        user: req.user,
        kind: "withdrawal",
        amount: req.body.amount,
        bankAccount: req.body.bankAccount,
      });
      res.status(201).json({ ok: true, transaction });
    } catch (error) {
      res.status(400).json({ ok: false, error: error.message });
    }
  }
);

router.get("/payment-requests", requireAuth, async (req, res) => {
  const rows = await Transaction.find({ userId: req.auth.userId })
    .sort({ createdAt: -1 })
    .limit(100)
    .lean();
  res.json({ ok: true, transactions: rows });
});

module.exports = router;


### FILE: src/routes/match.routes.js ###
const crypto = require("crypto");
const express = require("express");
const { z } = require("zod");
const User = require("../../models/User");
const Match = require("../../models/Match");
const MatchRequest = require("../../models/MatchRequest");
const { requireAuth } = require("../middleware/auth");
const { validate } = require("../middleware/validate");

const router = express.Router();

router.use(requireAuth);

router.get("/users/search", async (req, res) => {
  const q = String(req.query.q || "").trim();
  if (q.length < 3) {
    res.status(400).json({ ok: false, error: "Search needs at least 3 characters." });
    return;
  }

  const escaped = q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const users = await User.find({
    status: "active",
    userId: { $ne: req.auth.userId },
    $or: [{ userId: q }, { username: new RegExp(escaped, "i") }],
  })
    .select("userId username name rating wins losses matchStats")
    .limit(20)
    .lean();

  res.json({ ok: true, users });
});

const requestSchema = z.object({
  toUserId: z.string().trim().min(3),
  mode: z.enum(["friendly", "paid"]),
  stakeAmount: z.number().int().min(0).optional().default(0),
});

router.post("/requests", validate(requestSchema), async (req, res) => {
  if (req.body.toUserId === req.auth.userId) {
    res.status(400).json({ ok: false, error: "You cannot challenge yourself." });
    return;
  }

  const [fromUser, toUser] = await Promise.all([
    User.findOne({ userId: req.auth.userId }),
    User.findOne({ userId: req.body.toUserId, status: "active" }),
  ]);

  if (!toUser || !fromUser) {
    res.status(404).json({ ok: false, error: "User not found." });
    return;
  }

  if (req.body.mode === "paid" && req.body.stakeAmount <= 0) {
    res.status(400).json({ ok: false, error: "Paid matches need a positive stake." });
    return;
  }

  const request = await MatchRequest.create({
    requestId: `mr_${crypto.randomUUID()}`,
    fromUser: fromUser._id,
    fromUserId: fromUser.userId,
    toUser: toUser._id,
    toUserId: toUser.userId,
    mode: req.body.mode,
    stakeAmount: req.body.mode === "paid" ? req.body.stakeAmount : 0,
    expiresAt: new Date(Date.now() + 5 * 60 * 1000),
  });

  res.status(201).json({ ok: true, request });
});

router.get("/requests", async (req, res) => {
  const requests = await MatchRequest.find({
    $or: [{ fromUserId: req.auth.userId }, { toUserId: req.auth.userId }],
  })
    .sort({ createdAt: -1 })
    .limit(50)
    .lean();
  res.json({ ok: true, requests });
});

const responseSchema = z.object({
  action: z.enum(["accept", "reject", "cancel"]),
});

router.post("/requests/:requestId/respond", validate(responseSchema), async (req, res) => {
  const request = await MatchRequest.findOne({ requestId: req.params.requestId });
  if (!request || request.status !== "pending") {
    res.status(404).json({ ok: false, error: "Pending request not found." });
    return;
  }

  const isRecipient = request.toUserId === req.auth.userId;
  const isSender = request.fromUserId === req.auth.userId;
  if (req.body.action === "cancel" && !isSender) {
    res.status(403).json({ ok: false, error: "Only sender can cancel this request." });
    return;
  }
  if (req.body.action !== "cancel" && !isRecipient) {
    res.status(403).json({ ok: false, error: "Only recipient can respond." });
    return;
  }

  if (request.expiresAt.getTime() < Date.now()) {
    request.status = "expired";
    await request.save();
    res.status(400).json({ ok: false, error: "Request has expired." });
    return;
  }

  if (req.body.action === "reject" || req.body.action === "cancel") {
    request.status = req.body.action === "reject" ? "rejected" : "cancelled";
    request.respondedAt = new Date();
    await request.save();
    res.json({ ok: true, request });
    return;
  }

  const [fromUser, toUser] = await Promise.all([
    User.findOne({ userId: request.fromUserId }).lean(),
    User.findOne({ userId: request.toUserId }).lean(),
  ]);
  const match = await Match.create({
    matchId: `match_${crypto.randomUUID()}`,
    mode: request.mode,
    status: "accepted",
    stakeAmount: request.stakeAmount,
    totalPot: request.stakeAmount * 2,
    participants: [
      {
        user: fromUser?._id,
        userId: request.fromUserId,
        username: fromUser?.username || request.fromUserId,
        color: "white",
      },
      {
        user: toUser?._id,
        userId: request.toUserId,
        username: toUser?.username || request.toUserId,
        color: "black",
      },
    ],
  });

  request.status = "accepted";
  request.respondedAt = new Date();
  request.matchId = match.matchId;
  await request.save();

  res.json({ ok: true, request, match });
});

router.get("/history", async (req, res) => {
  const matches = await Match.find({ "participants.userId": req.auth.userId })
    .sort({ createdAt: -1 })
    .limit(100)
    .lean();
  res.json({ ok: true, matches });
});

module.exports = router;


### FILE: src/routes/admin.routes.js ###
const express = require("express");
const jwt = require("jsonwebtoken");
const { z } = require("zod");
const User = require("../../models/User");
const Transaction = require("../../models/Transaction");
const WalletLedger = require("../../models/WalletLedger");
const Match = require("../../models/Match");
const AdminAuditLog = require("../../models/AdminAuditLog");
const { requireAdmin } = require("../middleware/auth");
const { validate } = require("../middleware/validate");
const env = require("../config/env");
const { creditUser, debitUser, reviewTransaction } = require("../services/walletService");

const router = express.Router();

async function audit(req, action, targetType, targetId, before, after, metadata = {}) {
  await AdminAuditLog.create({
    adminUserId: req.auth?.userId || "unknown_admin",
    action,
    targetType,
    targetId,
    ip: req.ip,
    userAgent: req.get("user-agent") || "",
    before,
    after,
    metadata,
  });
}

function cookieOptions(req) {
  const secure =
    req.secure ||
    req.get("x-forwarded-proto") === "https" ||
    env.nodeEnv === "production";
  return [
    "HttpOnly",
    "Path=/",
    "SameSite=Lax",
    "Max-Age=28800",
    secure ? "Secure" : "",
  ]
    .filter(Boolean)
    .join("; ");
}

const adminLoginSchema = z.object({
  adminKey: z.string().trim().min(8),
});

router.post("/session/login", validate(adminLoginSchema), async (req, res) => {
  if (!env.adminApiKey || !env.jwtSecret) {
    res.status(503).json({
      ok: false,
      error: "ADMIN_API_KEY and JWT_SECRET must be configured on the server.",
    });
    return;
  }

  if (req.body.adminKey !== env.adminApiKey) {
    res.status(401).json({ ok: false, error: "Invalid admin key." });
    return;
  }

  const token = jwt.sign(
    { roles: ["admin"], method: "admin_key" },
    env.jwtSecret,
    {
      subject: "admin_panel",
      issuer: "chess-bet-app-admin",
      expiresIn: "8h",
    }
  );

  res.setHeader("Set-Cookie", `admin_session=${encodeURIComponent(token)}; ${cookieOptions(req)}`);
  res.json({ ok: true, admin: { userId: "admin_panel", roles: ["admin"] } });
});

router.post("/session/logout", (_req, res) => {
  res.setHeader(
    "Set-Cookie",
    "admin_session=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0"
  );
  res.json({ ok: true });
});

router.use(requireAdmin);

router.get("/session/me", (req, res) => {
  res.json({
    ok: true,
    admin: {
      userId: req.auth?.userId || "admin",
      roles: req.auth?.roles || ["admin"],
    },
  });
});

router.get("/dashboard", async (_req, res) => {
  const [
    totalUsers,
    activeUsers,
    suspendedUsers,
    bannedUsers,
    depositAgg,
    withdrawalAgg,
    pendingDeposits,
    pendingWithdrawals,
    matchAgg,
  ] = await Promise.all([
    User.countDocuments(),
    User.countDocuments({ status: "active" }),
    User.countDocuments({ status: "suspended" }),
    User.countDocuments({ status: "banned" }),
    Transaction.aggregate([
      { $match: { kind: "deposit", status: "approved" } },
      { $group: { _id: null, total: { $sum: "$amount" } } },
    ]),
    Transaction.aggregate([
      { $match: { kind: "withdrawal", status: "approved" } },
      { $group: { _id: null, total: { $sum: "$amount" } } },
    ]),
    Transaction.countDocuments({ kind: "deposit", status: "pending" }),
    Transaction.countDocuments({ kind: "withdrawal", status: "pending" }),
    Match.aggregate([
      { $match: { status: "completed" } },
      {
        $group: {
          _id: null,
          totalCommission: { $sum: "$commissionAmount" },
          totalMatches: { $sum: 1 },
          totalPot: { $sum: "$totalPot" },
        },
      },
    ]),
  ]);

  const totalDeposits = depositAgg[0]?.total || 0;
  const totalWithdrawals = withdrawalAgg[0]?.total || 0;
  const appProfit = matchAgg[0]?.totalCommission || 0;

  res.json({
    ok: true,
    analytics: {
      users: { total: totalUsers, active: activeUsers, suspended: suspendedUsers, banned: bannedUsers },
      money: { totalDeposits, totalWithdrawals, appProfit },
      pending: { deposits: pendingDeposits, withdrawals: pendingWithdrawals },
      matches: {
        totalCompleted: matchAgg[0]?.totalMatches || 0,
        totalPot: matchAgg[0]?.totalPot || 0,
      },
    },
  });
});

router.get("/users", async (req, res) => {
  const q = String(req.query.q || "").trim();
  const status = String(req.query.status || "").trim();
  const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 25));
  const filter = {};

  if (status) filter.status = status;
  if (q) {
    filter.$or = [
      { userId: q },
      { username: new RegExp(q, "i") },
      { phone: new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i") },
      { firebaseUid: q },
    ];
  }

  const users = await User.find(filter)
    .sort({ createdAt: -1 })
    .limit(limit)
    .select("-metadata")
    .lean();
  res.json({ ok: true, users });
});

router.get("/users/:userId", async (req, res) => {
  const user = await User.findOne({ userId: req.params.userId }).lean();
  if (!user) {
    res.status(404).json({ ok: false, error: "User not found." });
    return;
  }
  res.json({ ok: true, user });
});

const statusSchema = z.object({
  status: z.enum(["active", "suspended", "banned"]),
  reason: z.string().trim().max(200).optional().default(""),
});

router.patch("/users/:userId/status", validate(statusSchema), async (req, res) => {
  const before = await User.findOne({ userId: req.params.userId }).lean();
  if (!before) {
    res.status(404).json({ ok: false, error: "User not found." });
    return;
  }

  const after = await User.findOneAndUpdate(
    { userId: req.params.userId },
    {
      $set: {
        status: req.body.status,
        statusReason: req.body.reason,
        statusUpdatedAt: new Date(),
      },
    },
    { new: true }
  ).lean();

  await audit(req, "user_status_update", "user", req.params.userId, before, after);
  res.json({ ok: true, user: after });
});

router.get("/users/:userId/wallet-history", async (req, res) => {
  const rows = await WalletLedger.find({ userId: req.params.userId })
    .sort({ createdAt: -1 })
    .limit(100)
    .lean();
  res.json({ ok: true, transactions: rows });
});

router.get("/users/:userId/matches", async (req, res) => {
  const rows = await Match.find({ "participants.userId": req.params.userId })
    .sort({ createdAt: -1 })
    .limit(100)
    .lean();
  res.json({ ok: true, matches: rows });
});

const walletAdjustSchema = z.object({
  userId: z.string().trim().min(3),
  amount: z.number().int().positive(),
  note: z.string().trim().max(200).optional().default(""),
});

router.post("/wallet/credit", validate(walletAdjustSchema), async (req, res) => {
  try {
    const user = await creditUser({
      userId: req.body.userId,
      amount: req.body.amount,
      reason: "admin_credit",
      actorUserId: req.auth.userId,
      note: req.body.note,
    });
    await audit(req, "wallet_credit", "wallet", req.body.userId, null, {
      amount: req.body.amount,
      balance: user.wallet,
    });
    res.json({ ok: true, userId: user.userId, balance: user.wallet });
  } catch (error) {
    res.status(400).json({ ok: false, error: error.message });
  }
});

router.post("/wallet/debit", validate(walletAdjustSchema), async (req, res) => {
  try {
    const user = await debitUser({
      userId: req.body.userId,
      amount: req.body.amount,
      reason: "admin_debit",
      actorUserId: req.auth.userId,
      note: req.body.note,
    });
    await audit(req, "wallet_debit", "wallet", req.body.userId, null, {
      amount: req.body.amount,
      balance: user.wallet,
    });
    res.json({ ok: true, userId: user.userId, balance: user.wallet });
  } catch (error) {
    res.status(400).json({ ok: false, error: error.message });
  }
});

router.get("/transactions", async (req, res) => {
  const status = String(req.query.status || "").trim();
  const kind = String(req.query.kind || "").trim();
  const filter = {};
  if (status) filter.status = status;
  if (kind) filter.kind = kind;

  const rows = await Transaction.find(filter).sort({ createdAt: -1 }).limit(100).lean();
  res.json({ ok: true, transactions: rows });
});

const reviewSchema = z.object({
  status: z.enum(["approved", "rejected"]),
  rejectionReason: z.string().trim().max(200).optional().default(""),
});

router.post("/transactions/:transactionId/review", validate(reviewSchema), async (req, res) => {
  try {
    const transaction = await reviewTransaction({
      transactionId: req.params.transactionId,
      status: req.body.status,
      adminUserId: req.auth.userId,
      rejectionReason: req.body.rejectionReason,
    });
    await audit(
      req,
      `transaction_${req.body.status}`,
      "transaction",
      req.params.transactionId,
      null,
      transaction.toObject()
    );
    res.json({ ok: true, transaction });
  } catch (error) {
    res.status(400).json({ ok: false, error: error.message });
  }
});

module.exports = router;


### FILE: env.example ###
# Copy to .env on the VPS (or export these before `npm start`).
# MongoDB (Atlas or self-hosted) — required for real wallets / OTP users / settlements
MONGODB_URI=mongodb+srv://USER:PASS@cluster.mongodb.net/chessapp?retryWrites=true&w=majority

# HTTP + Socket.IO port (open this in VPS firewall)
PORT=4000

# Same secret as in your app backend for OTP login JWT (issuer chess-bet-app)
JWT_SECRET=change_me_long_random

# Admin: use header  X-Admin-Key: <value>  OR  Authorization: Bearer <value>
ADMIN_API_KEY=change_me_admin_key

# CORS: your app origin or * for testing
CORS_ORIGIN=*

# Optional: force login for paid queue (true / 1)
# SOCKET_REQUIRE_JWT_FOR_PAID=false

# OTP / SMS (only if you use /api/auth/* on this same process)
# OTP_HASH_SECRET=
# SMS_PROVIDER=console
# SMS_HTTP_URL=
# SMS_HTTP_BODY_TEMPLATE=

NODE_ENV=production


### FILE: VPS-DEPLOY.txt ###
================================================================================
  CHESS APP — DEPLOY THIS FOLDER TO YOUR VPS (Hostinger / Ubuntu / etc.)
================================================================================

WHAT TO UPLOAD
--------------
Upload the ENTIRE folder named "server" from your PC:

  ChessApp/server/

It MUST include:
  index.js
  package.json
  package-lock.json
  models/          (all .js files)
  src/             (all subfolders — routes, middleware, services, config)

Do NOT upload only index.js — the server will crash without models/ and src/.


ON THE VPS (Ubuntu example)
---------------------------
1) Install Node 20+ (example with NodeSource — adjust if you use nvm):

   curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
   sudo apt-get install -y nodejs

2) Create folder and upload files, e.g.:

   sudo mkdir -p /opt/chess-server
   sudo chown -R $USER:$USER /opt/chess-server
   # then scp/rsync/zip your local "server" folder contents into /opt/chess-server

3) Install dependencies:

   cd /opt/chess-server
   npm ci --omit=dev

4) Environment variables:

   cp env.example .env
   nano .env

   Fill at least: MONGODB_URI, JWT_SECRET, ADMIN_API_KEY, PORT

   The server loads a file named ".env" in this folder automatically (via dotenv).

   Test run:

   npm start

5) Open firewall for your port (4000 default):

   sudo ufw allow 4000/tcp
   sudo ufw reload

6) Keep it running (pick one):

   A) PM2 (install: sudo npm i -g pm2)

      cd /opt/chess-server
      pm2 start npm --name chess-server -- start
      pm2 save
      pm2 startup

   B) systemd: create a unit that runs `node /opt/chess-server/index.js`
      and set Environment= lines or EnvironmentFile=/opt/chess-server/.env


APP ON PHONE
------------
Point your React Native config to the VPS public IP (or domain) and PORT:

  ChessApp/config/vpsEndpoints.ts  →  VPS_HOST, SOCKET_PORT

If you put Nginx in front with HTTPS, set API_BASE_URL_OVERRIDE and
SOCKET_BASE_URL_OVERRIDE in the app config to your public URLs.


HEALTH CHECK
------------
  curl http://YOUR_VPS_IP:4000/health
  curl http://YOUR_VPS_IP:4000/


ADMIN LIVE (optional)
---------------------
  curl -H "x-admin-key: YOUR_ADMIN_API_KEY" http://YOUR_VPS_IP:4000/admin/live


================================================================================


### FILE: unbundle.js ###
/**
 * On VPS: upload PASTE-THIS-ON-VPS-FULL-BUNDLE.txt, then from same directory:
 *   node unbundle.js PASTE-THIS-ON-VPS-FULL-BUNDLE.txt
 * Then: npm ci --omit=dev && cp env.example .env && nano .env && npm start
 */
const fs = require("fs");
const path = require("path");

const bundlePath = path.resolve(process.argv[2] || "PASTE-THIS-ON-VPS-FULL-BUNDLE.txt");
if (!fs.existsSync(bundlePath)) {
  console.error("Usage: node unbundle.js <path-to-PASTE-THIS-ON-VPS-FULL-BUNDLE.txt>");
  process.exit(1);
}

const text = fs.readFileSync(bundlePath, "utf8");
const re = /^### FILE: (.+?) ###\r?\n/m;
const parts = text.split(re);
if (parts.length < 3) {
  console.error("Bundle format invalid (missing ### FILE: markers).");
  process.exit(1);
}

const preamble = parts[0];
console.log(preamble.slice(0, 200).replace(/\s+/g, " ").trim() + "...");

for (let i = 1; i < parts.length; i += 2) {
  const rel = parts[i].trim();
  let content = parts[i + 1] || "";
  content = content.replace(/^\r?\n+/, "").replace(/\r?\n+$/, "");
  const out = path.resolve(process.cwd(), rel);
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, content + (content.endsWith("\n") ? "" : "\n"), "utf8");
  console.log("wrote", rel);
}

console.log("\nDone. Run: npm ci --omit=dev");


### FILE: package-lock.json ###
{
  "name": "chess-app-socket",
  "version": "1.0.0",
  "lockfileVersion": 3,
  "requires": true,
  "packages": {
    "": {
      "name": "chess-app-socket",
      "version": "1.0.0",
      "dependencies": {
        "chess.js": "^1.4.0",
        "cors": "^2.8.6",
        "dotenv": "^16.6.1",
        "express": "^5.2.1",
        "express-rate-limit": "^8.5.2",
        "helmet": "^8.1.0",
        "jsonwebtoken": "^9.0.3",
        "mongoose": "^8.9.0",
        "socket.io": "^4.8.3",
        "zod": "^4.4.3"
      },
      "engines": {
        "node": ">=20.0.0"
      }
    },
    "node_modules/@mongodb-js/saslprep": {
      "version": "1.4.11",
      "resolved": "https://registry.npmjs.org/@mongodb-js/saslprep/-/saslprep-1.4.11.tgz",
      "integrity": "sha512-o9rAHc0IpIjuPSxRutWpE1F62x7n+4mVS4rCNHkzhIUMQcc18bb6xEq5wd2NdN0WjepIyXIppRshYI2kQDOZVA==",
      "license": "MIT",
      "dependencies": {
        "sparse-bitfield": "^3.0.3"
      }
    },
    "node_modules/@socket.io/component-emitter": {
      "version": "3.1.2",
      "resolved": "https://registry.npmjs.org/@socket.io/component-emitter/-/component-emitter-3.1.2.tgz",
      "integrity": "sha512-9BCxFwvbGg/RsZK9tjXd8s4UcwR0MWeFQ1XEKIQVVvAGJyINdrqKMcTRyLoK8Rse1GjzLV9cwjWV1olXRWEXVA==",
      "license": "MIT"
    },
    "node_modules/@types/cors": {
      "version": "2.8.19",
      "resolved": "https://registry.npmjs.org/@types/cors/-/cors-2.8.19.tgz",
      "integrity": "sha512-mFNylyeyqN93lfe/9CSxOGREz8cpzAhH+E93xJ4xWQf62V8sQ/24reV2nyzUWM6H6Xji+GGHpkbLe7pVoUEskg==",
      "license": "MIT",
      "dependencies": {
        "@types/node": "*"
      }
    },
    "node_modules/@types/node": {
      "version": "25.6.2",
      "resolved": "https://registry.npmjs.org/@types/node/-/node-25.6.2.tgz",
      "integrity": "sha512-sokuT28dxf9JT5Kady1fsXOvI4HVpjZa95NKT5y9PNTIrs2AsobR4GFAA90ZG8M+nxVRLysCXsVj6eGC7Vbrlw==",
      "license": "MIT",
      "dependencies": {
        "undici-types": "~7.19.0"
      }
    },
    "node_modules/@types/webidl-conversions": {
      "version": "7.0.3",
      "resolved": "https://registry.npmjs.org/@types/webidl-conversions/-/webidl-conversions-7.0.3.tgz",
      "integrity": "sha512-CiJJvcRtIgzadHCYXw7dqEnMNRjhGZlYK05Mj9OyktqV8uVT8fD2BFOB7S1uwBE3Kj2Z+4UyPmFw/Ixgw/LAlA==",
      "license": "MIT"
    },
    "node_modules/@types/whatwg-url": {
      "version": "11.0.5",
      "resolved": "https://registry.npmjs.org/@types/whatwg-url/-/whatwg-url-11.0.5.tgz",
      "integrity": "sha512-coYR071JRaHa+xoEvvYqvnIHaVqaYrLPbsufM9BF63HkwI5Lgmy2QR8Q5K/lYDYo5AK82wOvSOS0UsLTpTG7uQ==",
      "license": "MIT",
      "dependencies": {
        "@types/webidl-conversions": "*"
      }
    },
    "node_modules/@types/ws": {
      "version": "8.18.1",
      "resolved": "https://registry.npmjs.org/@types/ws/-/ws-8.18.1.tgz",
      "integrity": "sha512-ThVF6DCVhA8kUGy+aazFQ4kXQ7E1Ty7A3ypFOe0IcJV8O/M511G99AW24irKrW56Wt44yG9+ij8FaqoBGkuBXg==",
      "license": "MIT",
      "dependencies": {
        "@types/node": "*"
      }
    },
    "node_modules/accepts": {
      "version": "2.0.0",
      "resolved": "https://registry.npmjs.org/accepts/-/accepts-2.0.0.tgz",
      "integrity": "sha512-5cvg6CtKwfgdmVqY1WIiXKc3Q1bkRqGLi+2W/6ao+6Y7gu/RCwRuAhGEzh5B4KlszSuTLgZYuqFqo5bImjNKng==",
      "license": "MIT",
      "dependencies": {
        "mime-types": "^3.0.0",
        "negotiator": "^1.0.0"
      },
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/base64id": {
      "version": "2.0.0",
      "resolved": "https://registry.npmjs.org/base64id/-/base64id-2.0.0.tgz",
      "integrity": "sha512-lGe34o6EHj9y3Kts9R4ZYs/Gr+6N7MCaMlIFA3F1R2O5/m7K06AxfSeO5530PEERE6/WyEg3lsuyw4GHlPZHog==",
      "license": "MIT",
      "engines": {
        "node": "^4.5.0 || >= 5.9"
      }
    },
    "node_modules/bignumber.js": {
      "version": "9.3.1",
      "resolved": "https://registry.npmjs.org/bignumber.js/-/bignumber.js-9.3.1.tgz",
      "integrity": "sha512-Ko0uX15oIUS7wJ3Rb30Fs6SkVbLmPBAKdlm7q9+ak9bbIeFf0MwuBsQV6z7+X768/cHsfg+WlysDWJcmthjsjQ==",
      "license": "MIT",
      "optional": true,
      "peer": true,
      "engines": {
        "node": "*"
      }
    },
    "node_modules/body-parser": {
      "version": "2.2.2",
      "resolved": "https://registry.npmjs.org/body-parser/-/body-parser-2.2.2.tgz",
      "integrity": "sha512-oP5VkATKlNwcgvxi0vM0p/D3n2C3EReYVX+DNYs5TjZFn/oQt2j+4sVJtSMr18pdRr8wjTcBl6LoV+FUwzPmNA==",
      "license": "MIT",
      "dependencies": {
        "bytes": "^3.1.2",
        "content-type": "^1.0.5",
        "debug": "^4.4.3",
        "http-errors": "^2.0.0",
        "iconv-lite": "^0.7.0",
        "on-finished": "^2.4.1",
        "qs": "^6.14.1",
        "raw-body": "^3.0.1",
        "type-is": "^2.0.1"
      },
      "engines": {
        "node": ">=18"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/bson": {
      "version": "6.10.4",
      "resolved": "https://registry.npmjs.org/bson/-/bson-6.10.4.tgz",
      "integrity": "sha512-WIsKqkSC0ABoBJuT1LEX+2HEvNmNKKgnTAyd0fL8qzK4SH2i9NXg+t08YtdZp/V9IZ33cxe3iV4yM0qg8lMQng==",
      "license": "Apache-2.0",
      "engines": {
        "node": ">=16.20.1"
      }
    },
    "node_modules/buffer-equal-constant-time": {
      "version": "1.0.1",
      "resolved": "https://registry.npmjs.org/buffer-equal-constant-time/-/buffer-equal-constant-time-1.0.1.tgz",
      "integrity": "sha512-zRpUiDwd/xk6ADqPMATG8vc9VPrkck7T07OIx0gnjmJAnHnTVXNQG3vfvWNuiZIkwu9KrKdA1iJKfsfTVxE6NA==",
      "license": "BSD-3-Clause"
    },
    "node_modules/bytes": {
      "version": "3.1.2",
      "resolved": "https://registry.npmjs.org/bytes/-/bytes-3.1.2.tgz",
      "integrity": "sha512-/Nf7TyzTx6S3yRJObOAV7956r8cr2+Oj8AC5dt8wSP3BQAoeX58NoHyCU8P8zGkNXStjTSi6fzO6F0pBdcYbEg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/call-bind-apply-helpers": {
      "version": "1.0.2",
      "resolved": "https://registry.npmjs.org/call-bind-apply-helpers/-/call-bind-apply-helpers-1.0.2.tgz",
      "integrity": "sha512-Sp1ablJ0ivDkSzjcaJdxEunN5/XvksFJ2sMBFfq6x0ryhQV/2b/KwFe21cMpmHtPOSij8K99/wSfoEuTObmuMQ==",
      "license": "MIT",
      "dependencies": {
        "es-errors": "^1.3.0",
        "function-bind": "^1.1.2"
      },
      "engines": {
        "node": ">= 0.4"
      }
    },
    "node_modules/call-bound": {
      "version": "1.0.4",
      "resolved": "https://registry.npmjs.org/call-bound/-/call-bound-1.0.4.tgz",
      "integrity": "sha512-+ys997U96po4Kx/ABpBCqhA9EuxJaQWDQg7295H4hBphv3IZg0boBKuwYpt4YXp6MZ5AmZQnU/tyMTlRpaSejg==",
      "license": "MIT",
      "dependencies": {
        "call-bind-apply-helpers": "^1.0.2",
        "get-intrinsic": "^1.3.0"
      },
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/chess.js": {
      "version": "1.4.0",
      "resolved": "https://registry.npmjs.org/chess.js/-/chess.js-1.4.0.tgz",
      "integrity": "sha512-BBJgrrtKQOzFLonR0l+k64A98NLemPwNsCskwb+29bRwobUa4iTm51E1kwGPbWXAcfdDa18nad6vpPPKPWarqw==",
      "license": "BSD-2-Clause"
    },
    "node_modules/content-disposition": {
      "version": "1.1.0",
      "resolved": "https://registry.npmjs.org/content-disposition/-/content-disposition-1.1.0.tgz",
      "integrity": "sha512-5jRCH9Z/+DRP7rkvY83B+yGIGX96OYdJmzngqnw2SBSxqCFPd0w2km3s5iawpGX8krnwSGmF0FW5Nhr0Hfai3g==",
      "license": "MIT",
      "engines": {
        "node": ">=18"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/content-type": {
      "version": "1.0.5",
      "resolved": "https://registry.npmjs.org/content-type/-/content-type-1.0.5.tgz",
      "integrity": "sha512-nTjqfcBFEipKdXCv4YDQWCfmcLZKm81ldF0pAopTvyrFGVbcR6P/VAAd5G7N+0tTr8QqiU0tFadD6FK4NtJwOA==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/cookie": {
      "version": "0.7.2",
      "resolved": "https://registry.npmjs.org/cookie/-/cookie-0.7.2.tgz",
      "integrity": "sha512-yki5XnKuf750l50uGTllt6kKILY4nQ1eNIQatoXEByZ5dWgnKqbnqmTrBE5B4N7lrMJKQ2ytWMiTO2o0v6Ew/w==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/cookie-signature": {
      "version": "1.2.2",
      "resolved": "https://registry.npmjs.org/cookie-signature/-/cookie-signature-1.2.2.tgz",
      "integrity": "sha512-D76uU73ulSXrD1UXF4KE2TMxVVwhsnCgfAyTg9k8P6KGZjlXKrOLe4dJQKI3Bxi5wjesZoFXJWElNWBjPZMbhg==",
      "license": "MIT",
      "engines": {
        "node": ">=6.6.0"
      }
    },
    "node_modules/cors": {
      "version": "2.8.6",
      "resolved": "https://registry.npmjs.org/cors/-/cors-2.8.6.tgz",
      "integrity": "sha512-tJtZBBHA6vjIAaF6EnIaq6laBBP9aq/Y3ouVJjEfoHbRBcHBAHYcMh/w8LDrk2PvIMMq8gmopa5D4V8RmbrxGw==",
      "license": "MIT",
      "dependencies": {
        "object-assign": "^4",
        "vary": "^1"
      },
      "engines": {
        "node": ">= 0.10"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/debug": {
      "version": "4.4.3",
      "resolved": "https://registry.npmjs.org/debug/-/debug-4.4.3.tgz",
      "integrity": "sha512-RGwwWnwQvkVfavKVt22FGLw+xYSdzARwm0ru6DhTVA3umU5hZc28V3kO4stgYryrTlLpuvgI9GiijltAjNbcqA==",
      "license": "MIT",
      "dependencies": {
        "ms": "^2.1.3"
      },
      "engines": {
        "node": ">=6.0"
      },
      "peerDependenciesMeta": {
        "supports-color": {
          "optional": true
        }
      }
    },
    "node_modules/depd": {
      "version": "2.0.0",
      "resolved": "https://registry.npmjs.org/depd/-/depd-2.0.0.tgz",
      "integrity": "sha512-g7nH6P6dyDioJogAAGprGpCtVImJhpPk/roCzdb3fIh61/s/nPsfR6onyMwkCAR/OlC3yBC0lESvUoQEAssIrw==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/dotenv": {
      "version": "16.6.1",
      "resolved": "https://registry.npmjs.org/dotenv/-/dotenv-16.6.1.tgz",
      "integrity": "sha512-uBq4egWHTcTt33a72vpSG0z3HnPuIl6NqYcTrKEg2azoEyl2hpW0zqlxysq2pK9HlDIHyHyakeYaYnSAwd8bow==",
      "license": "BSD-2-Clause",
      "engines": {
        "node": ">=12"
      },
      "funding": {
        "url": "https://dotenvx.com"
      }
    },
    "node_modules/dunder-proto": {
      "version": "1.0.1",
      "resolved": "https://registry.npmjs.org/dunder-proto/-/dunder-proto-1.0.1.tgz",
      "integrity": "sha512-KIN/nDJBQRcXw0MLVhZE9iQHmG68qAVIBg9CqmUYjmQIhgij9U5MFvrqkUL5FbtyyzZuOeOt0zdeRe4UY7ct+A==",
      "license": "MIT",
      "dependencies": {
        "call-bind-apply-helpers": "^1.0.1",
        "es-errors": "^1.3.0",
        "gopd": "^1.2.0"
      },
      "engines": {
        "node": ">= 0.4"
      }
    },
    "node_modules/ecdsa-sig-formatter": {
      "version": "1.0.11",
      "resolved": "https://registry.npmjs.org/ecdsa-sig-formatter/-/ecdsa-sig-formatter-1.0.11.tgz",
      "integrity": "sha512-nagl3RYrbNv6kQkeJIpt6NJZy8twLB/2vtz6yN9Z4vRKHN4/QZJIEbqohALSgwKdnksuY3k5Addp5lg8sVoVcQ==",
      "license": "Apache-2.0",
      "dependencies": {
        "safe-buffer": "^5.0.1"
      }
    },
    "node_modules/ee-first": {
      "version": "1.1.1",
      "resolved": "https://registry.npmjs.org/ee-first/-/ee-first-1.1.1.tgz",
      "integrity": "sha512-WMwm9LhRUo+WUaRN+vRuETqG89IgZphVSNkdFgeb6sS/E4OrDIN7t48CAewSHXc6C8lefD8KKfr5vY61brQlow==",
      "license": "MIT"
    },
    "node_modules/encodeurl": {
      "version": "2.0.0",
      "resolved": "https://registry.npmjs.org/encodeurl/-/encodeurl-2.0.0.tgz",
      "integrity": "sha512-Q0n9HRi4m6JuGIV1eFlmvJB7ZEVxu93IrMyiMsGC0lrMJMWzRgx6WGquyfQgZVb31vhGgXnfmPNNXmxnOkRBrg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/engine.io": {
      "version": "6.6.7",
      "resolved": "https://registry.npmjs.org/engine.io/-/engine.io-6.6.7.tgz",
      "integrity": "sha512-DgOngfDKM2EviOH3Mr9m7ks1q8roetLy/IMmYthAYzbpInMbYc/GS+fWFA3rl1gvwKVsQrVV61fo5emD1y3OJQ==",
      "license": "MIT",
      "dependencies": {
        "@types/cors": "^2.8.12",
        "@types/node": ">=10.0.0",
        "@types/ws": "^8.5.12",
        "accepts": "~1.3.4",
        "base64id": "2.0.0",
        "cookie": "~0.7.2",
        "cors": "~2.8.5",
        "debug": "~4.4.1",
        "engine.io-parser": "~5.2.1",
        "ws": "~8.18.3"
      },
      "engines": {
        "node": ">=10.2.0"
      }
    },
    "node_modules/engine.io-parser": {
      "version": "5.2.3",
      "resolved": "https://registry.npmjs.org/engine.io-parser/-/engine.io-parser-5.2.3.tgz",
      "integrity": "sha512-HqD3yTBfnBxIrbnM1DoD6Pcq8NECnh8d4As1Qgh0z5Gg3jRRIqijury0CL3ghu/edArpUYiYqQiDUQBIs4np3Q==",
      "license": "MIT",
      "engines": {
        "node": ">=10.0.0"
      }
    },
    "node_modules/engine.io/node_modules/accepts": {
      "version": "1.3.8",
      "resolved": "https://registry.npmjs.org/accepts/-/accepts-1.3.8.tgz",
      "integrity": "sha512-PYAthTa2m2VKxuvSD3DPC/Gy+U+sOA1LAuT8mkmRuvw+NACSaeXEQ+NHcVF7rONl6qcaxV3Uuemwawk+7+SJLw==",
      "license": "MIT",
      "dependencies": {
        "mime-types": "~2.1.34",
        "negotiator": "0.6.3"
      },
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/engine.io/node_modules/mime-db": {
      "version": "1.52.0",
      "resolved": "https://registry.npmjs.org/mime-db/-/mime-db-1.52.0.tgz",
      "integrity": "sha512-sPU4uV7dYlvtWJxwwxHD0PuihVNiE7TyAbQ5SWxDCB9mUYvOgroQOwYQQOKPJ8CIbE+1ETVlOoK1UC2nU3gYvg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/engine.io/node_modules/mime-types": {
      "version": "2.1.35",
      "resolved": "https://registry.npmjs.org/mime-types/-/mime-types-2.1.35.tgz",
      "integrity": "sha512-ZDY+bPm5zTTF+YpCrAU9nK0UgICYPT0QtT1NZWFv4s++TNkcgVaT0g6+4R2uI4MjQjzysHB1zxuWL50hzaeXiw==",
      "license": "MIT",
      "dependencies": {
        "mime-db": "1.52.0"
      },
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/engine.io/node_modules/negotiator": {
      "version": "0.6.3",
      "resolved": "https://registry.npmjs.org/negotiator/-/negotiator-0.6.3.tgz",
      "integrity": "sha512-+EUsqGPLsM+j/zdChZjsnX51g4XrHFOIXwfnCVPGlQk/k5giakcKsuxCObBRu6DSm9opw/O6slWbJdghQM4bBg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/es-define-property": {
      "version": "1.0.1",
      "resolved": "https://registry.npmjs.org/es-define-property/-/es-define-property-1.0.1.tgz",
      "integrity": "sha512-e3nRfgfUZ4rNGL232gUgX06QNyyez04KdjFrF+LTRoOXmrOgFKDg4BCdsjW8EnT69eqdYGmRpJwiPVYNrCaW3g==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.4"
      }
    },
    "node_modules/es-errors": {
      "version": "1.3.0",
      "resolved": "https://registry.npmjs.org/es-errors/-/es-errors-1.3.0.tgz",
      "integrity": "sha512-Zf5H2Kxt2xjTvbJvP2ZWLEICxA6j+hAmMzIlypy4xcBg1vKVnx89Wy0GbS+kf5cwCVFFzdCFh2XSCFNULS6csw==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.4"
      }
    },
    "node_modules/es-object-atoms": {
      "version": "1.1.1",
      "resolved": "https://registry.npmjs.org/es-object-atoms/-/es-object-atoms-1.1.1.tgz",
      "integrity": "sha512-FGgH2h8zKNim9ljj7dankFPcICIK9Cp5bm+c2gQSYePhpaG5+esrLODihIorn+Pe6FGJzWhXQotPv73jTaldXA==",
      "license": "MIT",
      "dependencies": {
        "es-errors": "^1.3.0"
      },
      "engines": {
        "node": ">= 0.4"
      }
    },
    "node_modules/escape-html": {
      "version": "1.0.3",
      "resolved": "https://registry.npmjs.org/escape-html/-/escape-html-1.0.3.tgz",
      "integrity": "sha512-NiSupZ4OeuGwr68lGIeym/ksIZMJodUGOSCZ/FSnTxcrekbvqrgdUxlJOMpijaKZVjAJrWrGs/6Jy8OMuyj9ow==",
      "license": "MIT"
    },
    "node_modules/etag": {
      "version": "1.8.1",
      "resolved": "https://registry.npmjs.org/etag/-/etag-1.8.1.tgz",
      "integrity": "sha512-aIL5Fx7mawVa300al2BnEE4iNvo1qETxLrPI/o05L7z6go7fCw1J6EQmbK4FmJ2AS7kgVF/KEZWufBfdClMcPg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/express": {
      "version": "5.2.1",
      "resolved": "https://registry.npmjs.org/express/-/express-5.2.1.tgz",
      "integrity": "sha512-hIS4idWWai69NezIdRt2xFVofaF4j+6INOpJlVOLDO8zXGpUVEVzIYk12UUi2JzjEzWL3IOAxcTubgz9Po0yXw==",
      "license": "MIT",
      "dependencies": {
        "accepts": "^2.0.0",
        "body-parser": "^2.2.1",
        "content-disposition": "^1.0.0",
        "content-type": "^1.0.5",
        "cookie": "^0.7.1",
        "cookie-signature": "^1.2.1",
        "debug": "^4.4.0",
        "depd": "^2.0.0",
        "encodeurl": "^2.0.0",
        "escape-html": "^1.0.3",
        "etag": "^1.8.1",
        "finalhandler": "^2.1.0",
        "fresh": "^2.0.0",
        "http-errors": "^2.0.0",
        "merge-descriptors": "^2.0.0",
        "mime-types": "^3.0.0",
        "on-finished": "^2.4.1",
        "once": "^1.4.0",
        "parseurl": "^1.3.3",
        "proxy-addr": "^2.0.7",
        "qs": "^6.14.0",
        "range-parser": "^1.2.1",
        "router": "^2.2.0",
        "send": "^1.1.0",
        "serve-static": "^2.2.0",
        "statuses": "^2.0.1",
        "type-is": "^2.0.1",
        "vary": "^1.1.2"
      },
      "engines": {
        "node": ">= 18"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/express-rate-limit": {
      "version": "8.5.2",
      "resolved": "https://registry.npmjs.org/express-rate-limit/-/express-rate-limit-8.5.2.tgz",
      "integrity": "sha512-5Kb34ipNX694DH48vN9irak1Qx30nb0PLYHXfJgw4YEjiC3ZEmZJhwOp+VfiCYwFzvFTdB9QkArYS5kXa2cx2A==",
      "license": "MIT",
      "dependencies": {
        "ip-address": "^10.2.0"
      },
      "engines": {
        "node": ">= 16"
      },
      "funding": {
        "url": "https://github.com/sponsors/express-rate-limit"
      },
      "peerDependencies": {
        "express": ">= 4.11"
      }
    },
    "node_modules/extend": {
      "version": "3.0.2",
      "resolved": "https://registry.npmjs.org/extend/-/extend-3.0.2.tgz",
      "integrity": "sha512-fjquC59cD7CyW6urNXK0FBufkZcoiGG80wTuPujX590cB5Ttln20E2UB4S/WARVqhXffZl2LNgS+gQdPIIim/g==",
      "license": "MIT",
      "optional": true,
      "peer": true
    },
    "node_modules/finalhandler": {
      "version": "2.1.1",
      "resolved": "https://registry.npmjs.org/finalhandler/-/finalhandler-2.1.1.tgz",
      "integrity": "sha512-S8KoZgRZN+a5rNwqTxlZZePjT/4cnm0ROV70LedRHZ0p8u9fRID0hJUZQpkKLzro8LfmC8sx23bY6tVNxv8pQA==",
      "license": "MIT",
      "dependencies": {
        "debug": "^4.4.0",
        "encodeurl": "^2.0.0",
        "escape-html": "^1.0.3",
        "on-finished": "^2.4.1",
        "parseurl": "^1.3.3",
        "statuses": "^2.0.1"
      },
      "engines": {
        "node": ">= 18.0.0"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/forwarded": {
      "version": "0.2.0",
      "resolved": "https://registry.npmjs.org/forwarded/-/forwarded-0.2.0.tgz",
      "integrity": "sha512-buRG0fpBtRHSTCOASe6hD258tEubFoRLb4ZNA6NxMVHNw2gOcwHo9wyablzMzOA5z9xA9L1KNjk/Nt6MT9aYow==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/fresh": {
      "version": "2.0.0",
      "resolved": "https://registry.npmjs.org/fresh/-/fresh-2.0.0.tgz",
      "integrity": "sha512-Rx/WycZ60HOaqLKAi6cHRKKI7zxWbJ31MhntmtwMoaTeF7XFH9hhBp8vITaMidfljRQ6eYWCKkaTK+ykVJHP2A==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/function-bind": {
      "version": "1.1.2",
      "resolved": "https://registry.npmjs.org/function-bind/-/function-bind-1.1.2.tgz",
      "integrity": "sha512-7XHNxH7qX9xG5mIwxkhumTox/MIRNcOgDrxWsMt2pAr23WHp6MrRlN7FBSFpCpr+oVO0F744iUgR82nJMfG2SA==",
      "license": "MIT",
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/gcp-metadata": {
      "version": "5.3.0",
      "resolved": "https://registry.npmjs.org/gcp-metadata/-/gcp-metadata-5.3.0.tgz",
      "integrity": "sha512-FNTkdNEnBdlqF2oatizolQqNANMrcqJt6AAYt99B3y1aLLC8Hc5IOBb+ZnnzllodEEf6xMBp6wRcBbc16fa65w==",
      "license": "Apache-2.0",
      "optional": true,
      "peer": true,
      "dependencies": {
        "gaxios": "^5.0.0",
        "json-bigint": "^1.0.0"
      },
      "engines": {
        "node": ">=12"
      }
    },
    "node_modules/gcp-metadata/node_modules/agent-base": {
      "version": "6.0.2",
      "resolved": "https://registry.npmjs.org/agent-base/-/agent-base-6.0.2.tgz",
      "integrity": "sha512-RZNwNclF7+MS/8bDg70amg32dyeZGZxiDuQmZxKLAlQjr3jGyLx+4Kkk58UO7D2QdgFIQCovuSuZESne6RG6XQ==",
      "license": "MIT",
      "optional": true,
      "peer": true,
      "dependencies": {
        "debug": "4"
      },
      "engines": {
        "node": ">= 6.0.0"
      }
    },
    "node_modules/gcp-metadata/node_modules/gaxios": {
      "version": "5.1.3",
      "resolved": "https://registry.npmjs.org/gaxios/-/gaxios-5.1.3.tgz",
      "integrity": "sha512-95hVgBRgEIRQQQHIbnxBXeHbW4TqFk4ZDJW7wmVtvYar72FdhRIo1UGOLS2eRAKCPEdPBWu+M7+A33D9CdX9rA==",
      "license": "Apache-2.0",
      "optional": true,
      "peer": true,
      "dependencies": {
        "extend": "^3.0.2",
        "https-proxy-agent": "^5.0.0",
        "is-stream": "^2.0.0",
        "node-fetch": "^2.6.9"
      },
      "engines": {
        "node": ">=12"
      }
    },
    "node_modules/gcp-metadata/node_modules/https-proxy-agent": {
      "version": "5.0.1",
      "resolved": "https://registry.npmjs.org/https-proxy-agent/-/https-proxy-agent-5.0.1.tgz",
      "integrity": "sha512-dFcAjpTQFgoLMzC2VwU+C/CbS7uRL0lWmxDITmqm7C+7F0Odmj6s9l6alZc6AELXhrnggM2CeWSXHGOdX2YtwA==",
      "license": "MIT",
      "optional": true,
      "peer": true,
      "dependencies": {
        "agent-base": "6",
        "debug": "4"
      },
      "engines": {
        "node": ">= 6"
      }
    },
    "node_modules/get-intrinsic": {
      "version": "1.3.0",
      "resolved": "https://registry.npmjs.org/get-intrinsic/-/get-intrinsic-1.3.0.tgz",
      "integrity": "sha512-9fSjSaos/fRIVIp+xSJlE6lfwhES7LNtKaCBIamHsjr2na1BiABJPo0mOjjz8GJDURarmCPGqaiVg5mfjb98CQ==",
      "license": "MIT",
      "dependencies": {
        "call-bind-apply-helpers": "^1.0.2",
        "es-define-property": "^1.0.1",
        "es-errors": "^1.3.0",
        "es-object-atoms": "^1.1.1",
        "function-bind": "^1.1.2",
        "get-proto": "^1.0.1",
        "gopd": "^1.2.0",
        "has-symbols": "^1.1.0",
        "hasown": "^2.0.2",
        "math-intrinsics": "^1.1.0"
      },
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/get-proto": {
      "version": "1.0.1",
      "resolved": "https://registry.npmjs.org/get-proto/-/get-proto-1.0.1.tgz",
      "integrity": "sha512-sTSfBjoXBp89JvIKIefqw7U2CCebsc74kiY6awiGogKtoSGbgjYE/G/+l9sF3MWFPNc9IcoOC4ODfKHfxFmp0g==",
      "license": "MIT",
      "dependencies": {
        "dunder-proto": "^1.0.1",
        "es-object-atoms": "^1.0.0"
      },
      "engines": {
        "node": ">= 0.4"
      }
    },
    "node_modules/gopd": {
      "version": "1.2.0",
      "resolved": "https://registry.npmjs.org/gopd/-/gopd-1.2.0.tgz",
      "integrity": "sha512-ZUKRh6/kUFoAiTAtTYPZJ3hw9wNxx+BIBOijnlG9PnrJsCcSjs1wyyD6vJpaYtgnzDrKYRSqf3OO6Rfa93xsRg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/has-symbols": {
      "version": "1.1.0",
      "resolved": "https://registry.npmjs.org/has-symbols/-/has-symbols-1.1.0.tgz",
      "integrity": "sha512-1cDNdwJ2Jaohmb3sg4OmKaMBwuC48sYni5HUw2DvsC8LjGTLK9h+eb1X6RyuOHe4hT0ULCW68iomhjUoKUqlPQ==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/hasown": {
      "version": "2.0.3",
      "resolved": "https://registry.npmjs.org/hasown/-/hasown-2.0.3.tgz",
      "integrity": "sha512-ej4AhfhfL2Q2zpMmLo7U1Uv9+PyhIZpgQLGT1F9miIGmiCJIoCgSmczFdrc97mWT4kVY72KA+WnnhJ5pghSvSg==",
      "license": "MIT",
      "dependencies": {
        "function-bind": "^1.1.2"
      },
      "engines": {
        "node": ">= 0.4"
      }
    },
    "node_modules/helmet": {
      "version": "8.1.0",
      "resolved": "https://registry.npmjs.org/helmet/-/helmet-8.1.0.tgz",
      "integrity": "sha512-jOiHyAZsmnr8LqoPGmCjYAaiuWwjAPLgY8ZX2XrmHawt99/u1y6RgrZMTeoPfpUbV96HOalYgz1qzkRbw54Pmg==",
      "license": "MIT",
      "engines": {
        "node": ">=18.0.0"
      }
    },
    "node_modules/http-errors": {
      "version": "2.0.1",
      "resolved": "https://registry.npmjs.org/http-errors/-/http-errors-2.0.1.tgz",
      "integrity": "sha512-4FbRdAX+bSdmo4AUFuS0WNiPz8NgFt+r8ThgNWmlrjQjt1Q7ZR9+zTlce2859x4KSXrwIsaeTqDoKQmtP8pLmQ==",
      "license": "MIT",
      "dependencies": {
        "depd": "~2.0.0",
        "inherits": "~2.0.4",
        "setprototypeof": "~1.2.0",
        "statuses": "~2.0.2",
        "toidentifier": "~1.0.1"
      },
      "engines": {
        "node": ">= 0.8"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/iconv-lite": {
      "version": "0.7.2",
      "resolved": "https://registry.npmjs.org/iconv-lite/-/iconv-lite-0.7.2.tgz",
      "integrity": "sha512-im9DjEDQ55s9fL4EYzOAv0yMqmMBSZp6G0VvFyTMPKWxiSBHUj9NW/qqLmXUwXrrM7AvqSlTCfvqRb0cM8yYqw==",
      "license": "MIT",
      "dependencies": {
        "safer-buffer": ">= 2.1.2 < 3.0.0"
      },
      "engines": {
        "node": ">=0.10.0"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/inherits": {
      "version": "2.0.4",
      "resolved": "https://registry.npmjs.org/inherits/-/inherits-2.0.4.tgz",
      "integrity": "sha512-k/vGaX4/Yla3WzyMCvTQOXYeIHvqOKtnqBduzTHpzpQZzAskKMhZ2K+EnBiSM9zGSoIFeMpXKxa4dYeZIQqewQ==",
      "license": "ISC"
    },
    "node_modules/ip-address": {
      "version": "10.2.0",
      "resolved": "https://registry.npmjs.org/ip-address/-/ip-address-10.2.0.tgz",
      "integrity": "sha512-/+S6j4E9AHvW9SWMSEY9Xfy66O5PWvVEJ08O0y5JGyEKQpojb0K0GKpz/v5HJ/G0vi3D2sjGK78119oXZeE0qA==",
      "license": "MIT",
      "engines": {
        "node": ">= 12"
      }
    },
    "node_modules/ipaddr.js": {
      "version": "1.9.1",
      "resolved": "https://registry.npmjs.org/ipaddr.js/-/ipaddr.js-1.9.1.tgz",
      "integrity": "sha512-0KI/607xoxSToH7GjN1FfSbLoU0+btTicjsQSWQlh/hZykN8KpmMf7uYwPW3R+akZ6R/w18ZlXSHBYXiYUPO3g==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.10"
      }
    },
    "node_modules/is-promise": {
      "version": "4.0.0",
      "resolved": "https://registry.npmjs.org/is-promise/-/is-promise-4.0.0.tgz",
      "integrity": "sha512-hvpoI6korhJMnej285dSg6nu1+e6uxs7zG3BYAm5byqDsgJNWwxzM6z6iZiAgQR4TJ30JmBTOwqZUw3WlyH3AQ==",
      "license": "MIT"
    },
    "node_modules/is-stream": {
      "version": "2.0.1",
      "resolved": "https://registry.npmjs.org/is-stream/-/is-stream-2.0.1.tgz",
      "integrity": "sha512-hFoiJiTl63nn+kstHGBtewWSKnQLpyb155KHheA1l39uvtO9nWIop1p3udqPcUd/xbF1VLMO4n7OI6p7RbngDg==",
      "license": "MIT",
      "optional": true,
      "peer": true,
      "engines": {
        "node": ">=8"
      },
      "funding": {
        "url": "https://github.com/sponsors/sindresorhus"
      }
    },
    "node_modules/json-bigint": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/json-bigint/-/json-bigint-1.0.0.tgz",
      "integrity": "sha512-SiPv/8VpZuWbvLSMtTDU8hEfrZWg/mH/nV/b4o0CYbSxu1UIQPLdwKOCIyLQX+VIPO5vrLX3i8qtqFyhdPSUSQ==",
      "license": "MIT",
      "optional": true,
      "peer": true,
      "dependencies": {
        "bignumber.js": "^9.0.0"
      }
    },
    "node_modules/jsonwebtoken": {
      "version": "9.0.3",
      "resolved": "https://registry.npmjs.org/jsonwebtoken/-/jsonwebtoken-9.0.3.tgz",
      "integrity": "sha512-MT/xP0CrubFRNLNKvxJ2BYfy53Zkm++5bX9dtuPbqAeQpTVe0MQTFhao8+Cp//EmJp244xt6Drw/GVEGCUj40g==",
      "license": "MIT",
      "dependencies": {
        "jws": "^4.0.1",
        "lodash.includes": "^4.3.0",
        "lodash.isboolean": "^3.0.3",
        "lodash.isinteger": "^4.0.4",
        "lodash.isnumber": "^3.0.3",
        "lodash.isplainobject": "^4.0.6",
        "lodash.isstring": "^4.0.1",
        "lodash.once": "^4.0.0",
        "ms": "^2.1.1",
        "semver": "^7.5.4"
      },
      "engines": {
        "node": ">=12",
        "npm": ">=6"
      }
    },
    "node_modules/jwa": {
      "version": "2.0.1",
      "resolved": "https://registry.npmjs.org/jwa/-/jwa-2.0.1.tgz",
      "integrity": "sha512-hRF04fqJIP8Abbkq5NKGN0Bbr3JxlQ+qhZufXVr0DvujKy93ZCbXZMHDL4EOtodSbCWxOqR8MS1tXA5hwqCXDg==",
      "license": "MIT",
      "dependencies": {
        "buffer-equal-constant-time": "^1.0.1",
        "ecdsa-sig-formatter": "1.0.11",
        "safe-buffer": "^5.0.1"
      }
    },
    "node_modules/jws": {
      "version": "4.0.1",
      "resolved": "https://registry.npmjs.org/jws/-/jws-4.0.1.tgz",
      "integrity": "sha512-EKI/M/yqPncGUUh44xz0PxSidXFr/+r0pA70+gIYhjv+et7yxM+s29Y+VGDkovRofQem0fs7Uvf4+YmAdyRduA==",
      "license": "MIT",
      "dependencies": {
        "jwa": "^2.0.1",
        "safe-buffer": "^5.0.1"
      }
    },
    "node_modules/kareem": {
      "version": "2.6.3",
      "resolved": "https://registry.npmjs.org/kareem/-/kareem-2.6.3.tgz",
      "integrity": "sha512-C3iHfuGUXK2u8/ipq9LfjFfXFxAZMQJJq7vLS45r3D9Y2xQ/m4S8zaR4zMLFWh9AsNPXmcFfUDhTEO8UIC/V6Q==",
      "license": "Apache-2.0",
      "engines": {
        "node": ">=12.0.0"
      }
    },
    "node_modules/lodash.includes": {
      "version": "4.3.0",
      "resolved": "https://registry.npmjs.org/lodash.includes/-/lodash.includes-4.3.0.tgz",
      "integrity": "sha512-W3Bx6mdkRTGtlJISOvVD/lbqjTlPPUDTMnlXZFnVwi9NKJ6tiAk6LVdlhZMm17VZisqhKcgzpO5Wz91PCt5b0w==",
      "license": "MIT"
    },
    "node_modules/lodash.isboolean": {
      "version": "3.0.3",
      "resolved": "https://registry.npmjs.org/lodash.isboolean/-/lodash.isboolean-3.0.3.tgz",
      "integrity": "sha512-Bz5mupy2SVbPHURB98VAcw+aHh4vRV5IPNhILUCsOzRmsTmSQ17jIuqopAentWoehktxGd9e/hbIXq980/1QJg==",
      "license": "MIT"
    },
    "node_modules/lodash.isinteger": {
      "version": "4.0.4",
      "resolved": "https://registry.npmjs.org/lodash.isinteger/-/lodash.isinteger-4.0.4.tgz",
      "integrity": "sha512-DBwtEWN2caHQ9/imiNeEA5ys1JoRtRfY3d7V9wkqtbycnAmTvRRmbHKDV4a0EYc678/dia0jrte4tjYwVBaZUA==",
      "license": "MIT"
    },
    "node_modules/lodash.isnumber": {
      "version": "3.0.3",
      "resolved": "https://registry.npmjs.org/lodash.isnumber/-/lodash.isnumber-3.0.3.tgz",
      "integrity": "sha512-QYqzpfwO3/CWf3XP+Z+tkQsfaLL/EnUlXWVkIk5FUPc4sBdTehEqZONuyRt2P67PXAk+NXmTBcc97zw9t1FQrw==",
      "license": "MIT"
    },
    "node_modules/lodash.isplainobject": {
      "version": "4.0.6",
      "resolved": "https://registry.npmjs.org/lodash.isplainobject/-/lodash.isplainobject-4.0.6.tgz",
      "integrity": "sha512-oSXzaWypCMHkPC3NvBEaPHf0KsA5mvPrOPgQWDsbg8n7orZ290M0BmC/jgRZ4vcJ6DTAhjrsSYgdsW/F+MFOBA==",
      "license": "MIT"
    },
    "node_modules/lodash.isstring": {
      "version": "4.0.1",
      "resolved": "https://registry.npmjs.org/lodash.isstring/-/lodash.isstring-4.0.1.tgz",
      "integrity": "sha512-0wJxfxH1wgO3GrbuP+dTTk7op+6L41QCXbGINEmD+ny/G/eCqGzxyCsh7159S+mgDDcoarnBw6PC1PS5+wUGgw==",
      "license": "MIT"
    },
    "node_modules/lodash.once": {
      "version": "4.1.1",
      "resolved": "https://registry.npmjs.org/lodash.once/-/lodash.once-4.1.1.tgz",
      "integrity": "sha512-Sb487aTOCr9drQVL8pIxOzVhafOjZN9UU54hiN8PU3uAiSV7lx1yYNpbNmex2PK6dSJoNTSJUUswT651yww3Mg==",
      "license": "MIT"
    },
    "node_modules/math-intrinsics": {
      "version": "1.1.0",
      "resolved": "https://registry.npmjs.org/math-intrinsics/-/math-intrinsics-1.1.0.tgz",
      "integrity": "sha512-/IXtbwEk5HTPyEwyKX6hGkYXxM9nbj64B+ilVJnC/R6B0pH5G4V3b0pVbL7DBj4tkhBAppbQUlf6F6Xl9LHu1g==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.4"
      }
    },
    "node_modules/media-typer": {
      "version": "1.1.0",
      "resolved": "https://registry.npmjs.org/media-typer/-/media-typer-1.1.0.tgz",
      "integrity": "sha512-aisnrDP4GNe06UcKFnV5bfMNPBUw4jsLGaWwWfnH3v02GnBuXX2MCVn5RbrWo0j3pczUilYblq7fQ7Nw2t5XKw==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/memory-pager": {
      "version": "1.5.0",
      "resolved": "https://registry.npmjs.org/memory-pager/-/memory-pager-1.5.0.tgz",
      "integrity": "sha512-ZS4Bp4r/Zoeq6+NLJpP+0Zzm0pR8whtGPf1XExKLJBAczGMnSi3It14OiNCStjQjM6NU1okjQGSxgEZN8eBYKg==",
      "license": "MIT"
    },
    "node_modules/merge-descriptors": {
      "version": "2.0.0",
      "resolved": "https://registry.npmjs.org/merge-descriptors/-/merge-descriptors-2.0.0.tgz",
      "integrity": "sha512-Snk314V5ayFLhp3fkUREub6WtjBfPdCPY1Ln8/8munuLuiYhsABgBVWsozAG+MWMbVEvcdcpbi9R7ww22l9Q3g==",
      "license": "MIT",
      "engines": {
        "node": ">=18"
      },
      "funding": {
        "url": "https://github.com/sponsors/sindresorhus"
      }
    },
    "node_modules/mime-db": {
      "version": "1.54.0",
      "resolved": "https://registry.npmjs.org/mime-db/-/mime-db-1.54.0.tgz",
      "integrity": "sha512-aU5EJuIN2WDemCcAp2vFBfp/m4EAhWJnUNSSw0ixs7/kXbd6Pg64EmwJkNdFhB8aWt1sH2CTXrLxo/iAGV3oPQ==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/mime-types": {
      "version": "3.0.2",
      "resolved": "https://registry.npmjs.org/mime-types/-/mime-types-3.0.2.tgz",
      "integrity": "sha512-Lbgzdk0h4juoQ9fCKXW4by0UJqj+nOOrI9MJ1sSj4nI8aI2eo1qmvQEie4VD1glsS250n15LsWsYtCugiStS5A==",
      "license": "MIT",
      "dependencies": {
        "mime-db": "^1.54.0"
      },
      "engines": {
        "node": ">=18"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/mongodb": {
      "version": "6.20.0",
      "resolved": "https://registry.npmjs.org/mongodb/-/mongodb-6.20.0.tgz",
      "integrity": "sha512-Tl6MEIU3K4Rq3TSHd+sZQqRBoGlFsOgNrH5ltAcFBV62Re3Fd+FcaVf8uSEQFOJ51SDowDVttBTONMfoYWrWlQ==",
      "license": "Apache-2.0",
      "dependencies": {
        "@mongodb-js/saslprep": "^1.3.0",
        "bson": "^6.10.4",
        "mongodb-connection-string-url": "^3.0.2"
      },
      "engines": {
        "node": ">=16.20.1"
      },
      "peerDependencies": {
        "@aws-sdk/credential-providers": "^3.188.0",
        "@mongodb-js/zstd": "^1.1.0 || ^2.0.0",
        "gcp-metadata": "^5.2.0",
        "kerberos": "^2.0.1",
        "mongodb-client-encryption": ">=6.0.0 <7",
        "snappy": "^7.3.2",
        "socks": "^2.7.1"
      },
      "peerDependenciesMeta": {
        "@aws-sdk/credential-providers": {
          "optional": true
        },
        "@mongodb-js/zstd": {
          "optional": true
        },
        "gcp-metadata": {
          "optional": true
        },
        "kerberos": {
          "optional": true
        },
        "mongodb-client-encryption": {
          "optional": true
        },
        "snappy": {
          "optional": true
        },
        "socks": {
          "optional": true
        }
      }
    },
    "node_modules/mongodb-connection-string-url": {
      "version": "3.0.2",
      "resolved": "https://registry.npmjs.org/mongodb-connection-string-url/-/mongodb-connection-string-url-3.0.2.tgz",
      "integrity": "sha512-rMO7CGo/9BFwyZABcKAWL8UJwH/Kc2x0g72uhDWzG48URRax5TCIcJ7Rc3RZqffZzO/Gwff/jyKwCU9TN8gehA==",
      "license": "Apache-2.0",
      "dependencies": {
        "@types/whatwg-url": "^11.0.2",
        "whatwg-url": "^14.1.0 || ^13.0.0"
      }
    },
    "node_modules/mongoose": {
      "version": "8.23.1",
      "resolved": "https://registry.npmjs.org/mongoose/-/mongoose-8.23.1.tgz",
      "integrity": "sha512-gHSPD8qEwRmiXapK17hEnFWZdcFENMegHTcw5XIIg2+7R8eXQvdwSiMpD/A2oG8tKzFLLHyRXd8/eaDPAVwZgQ==",
      "license": "MIT",
      "dependencies": {
        "bson": "^6.10.4",
        "kareem": "2.6.3",
        "mongodb": "~6.20.0",
        "mpath": "0.9.0",
        "mquery": "5.0.0",
        "ms": "2.1.3",
        "sift": "17.1.3"
      },
      "engines": {
        "node": ">=16.20.1"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/mongoose"
      }
    },
    "node_modules/mpath": {
      "version": "0.9.0",
      "resolved": "https://registry.npmjs.org/mpath/-/mpath-0.9.0.tgz",
      "integrity": "sha512-ikJRQTk8hw5DEoFVxHG1Gn9T/xcjtdnOKIU1JTmGjZZlg9LST2mBLmcX3/ICIbgJydT2GOc15RnNy5mHmzfSew==",
      "license": "MIT",
      "engines": {
        "node": ">=4.0.0"
      }
    },
    "node_modules/mquery": {
      "version": "5.0.0",
      "resolved": "https://registry.npmjs.org/mquery/-/mquery-5.0.0.tgz",
      "integrity": "sha512-iQMncpmEK8R8ncT8HJGsGc9Dsp8xcgYMVSbs5jgnm1lFHTZqMJTUWTDx1LBO8+mK3tPNZWFLBghQEIOULSTHZg==",
      "license": "MIT",
      "dependencies": {
        "debug": "4.x"
      },
      "engines": {
        "node": ">=14.0.0"
      }
    },
    "node_modules/ms": {
      "version": "2.1.3",
      "resolved": "https://registry.npmjs.org/ms/-/ms-2.1.3.tgz",
      "integrity": "sha512-6FlzubTLZG3J2a/NVCAleEhjzq5oxgHyaCU9yYXvcLsvoVaHJq/s5xXI6/XXP6tz7R9xAOtHnSO/tXtF3WRTlA==",
      "license": "MIT"
    },
    "node_modules/negotiator": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/negotiator/-/negotiator-1.0.0.tgz",
      "integrity": "sha512-8Ofs/AUQh8MaEcrlq5xOX0CQ9ypTF5dl78mjlMNfOK08fzpgTHQRQPBxcPlEtIw0yRpws+Zo/3r+5WRby7u3Gg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/node-fetch": {
      "version": "2.7.0",
      "resolved": "https://registry.npmjs.org/node-fetch/-/node-fetch-2.7.0.tgz",
      "integrity": "sha512-c4FRfUm/dbcWZ7U+1Wq0AwCyFL+3nt2bEw05wfxSz+DWpWsitgmSgYmy2dQdWyKC1694ELPqMs/YzUSNozLt8A==",
      "license": "MIT",
      "optional": true,
      "peer": true,
      "dependencies": {
        "whatwg-url": "^5.0.0"
      },
      "engines": {
        "node": "4.x || >=6.0.0"
      },
      "peerDependencies": {
        "encoding": "^0.1.0"
      },
      "peerDependenciesMeta": {
        "encoding": {
          "optional": true
        }
      }
    },
    "node_modules/node-fetch/node_modules/tr46": {
      "version": "0.0.3",
      "resolved": "https://registry.npmjs.org/tr46/-/tr46-0.0.3.tgz",
      "integrity": "sha512-N3WMsuqV66lT30CrXNbEjx4GEwlow3v6rr4mCcv6prnfwhS01rkgyFdjPNBYd9br7LpXV1+Emh01fHnq2Gdgrw==",
      "license": "MIT",
      "optional": true,
      "peer": true
    },
    "node_modules/node-fetch/node_modules/webidl-conversions": {
      "version": "3.0.1",
      "resolved": "https://registry.npmjs.org/webidl-conversions/-/webidl-conversions-3.0.1.tgz",
      "integrity": "sha512-2JAn3z8AR6rjK8Sm8orRC0h/bcl/DqL7tRPdGZ4I1CjdF+EaMLmYxBHyXuKL849eucPFhvBoxMsflfOb8kxaeQ==",
      "license": "BSD-2-Clause",
      "optional": true,
      "peer": true
    },
    "node_modules/node-fetch/node_modules/whatwg-url": {
      "version": "5.0.0",
      "resolved": "https://registry.npmjs.org/whatwg-url/-/whatwg-url-5.0.0.tgz",
      "integrity": "sha512-saE57nupxk6v3HY35+jzBwYa0rKSy0XR8JSxZPwgLr7ys0IBzhGviA1/TUGJLmSVqs8pb9AnvICXEuOHLprYTw==",
      "license": "MIT",
      "optional": true,
      "peer": true,
      "dependencies": {
        "tr46": "~0.0.3",
        "webidl-conversions": "^3.0.0"
      }
    },
    "node_modules/object-assign": {
      "version": "4.1.1",
      "resolved": "https://registry.npmjs.org/object-assign/-/object-assign-4.1.1.tgz",
      "integrity": "sha512-rJgTQnkUnH1sFw8yT6VSU3zD3sWmu6sZhIseY8VX+GRu3P6F7Fu+JNDoXfklElbLJSnc3FUQHVe4cU5hj+BcUg==",
      "license": "MIT",
      "engines": {
        "node": ">=0.10.0"
      }
    },
    "node_modules/object-inspect": {
      "version": "1.13.4",
      "resolved": "https://registry.npmjs.org/object-inspect/-/object-inspect-1.13.4.tgz",
      "integrity": "sha512-W67iLl4J2EXEGTbfeHCffrjDfitvLANg0UlX3wFUUSTx92KXRFegMHUVgSqE+wvhAbi4WqjGg9czysTV2Epbew==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/on-finished": {
      "version": "2.4.1",
      "resolved": "https://registry.npmjs.org/on-finished/-/on-finished-2.4.1.tgz",
      "integrity": "sha512-oVlzkg3ENAhCk2zdv7IJwd/QUD4z2RxRwpkcGY8psCVcCYZNq4wYnVWALHM+brtuJjePWiYF/ClmuDr8Ch5+kg==",
      "license": "MIT",
      "dependencies": {
        "ee-first": "1.1.1"
      },
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/once": {
      "version": "1.4.0",
      "resolved": "https://registry.npmjs.org/once/-/once-1.4.0.tgz",
      "integrity": "sha512-lNaJgI+2Q5URQBkccEKHTQOPaXdUxnZZElQTZY0MFUAuaEqe1E+Nyvgdz/aIyNi6Z9MzO5dv1H8n58/GELp3+w==",
      "license": "ISC",
      "dependencies": {
        "wrappy": "1"
      }
    },
    "node_modules/parseurl": {
      "version": "1.3.3",
      "resolved": "https://registry.npmjs.org/parseurl/-/parseurl-1.3.3.tgz",
      "integrity": "sha512-CiyeOxFT/JZyN5m0z9PfXw4SCBJ6Sygz1Dpl0wqjlhDEGGBP1GnsUVEL0p63hoG1fcj3fHynXi9NYO4nWOL+qQ==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/path-to-regexp": {
      "version": "8.4.2",
      "resolved": "https://registry.npmjs.org/path-to-regexp/-/path-to-regexp-8.4.2.tgz",
      "integrity": "sha512-qRcuIdP69NPm4qbACK+aDogI5CBDMi1jKe0ry5rSQJz8JVLsC7jV8XpiJjGRLLol3N+R5ihGYcrPLTno6pAdBA==",
      "license": "MIT",
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/proxy-addr": {
      "version": "2.0.7",
      "resolved": "https://registry.npmjs.org/proxy-addr/-/proxy-addr-2.0.7.tgz",
      "integrity": "sha512-llQsMLSUDUPT44jdrU/O37qlnifitDP+ZwrmmZcoSKyLKvtZxpyV0n2/bD/N4tBAAZ/gJEdZU7KMraoK1+XYAg==",
      "license": "MIT",
      "dependencies": {
        "forwarded": "0.2.0",
        "ipaddr.js": "1.9.1"
      },
      "engines": {
        "node": ">= 0.10"
      }
    },
    "node_modules/punycode": {
      "version": "2.3.1",
      "resolved": "https://registry.npmjs.org/punycode/-/punycode-2.3.1.tgz",
      "integrity": "sha512-vYt7UD1U9Wg6138shLtLOvdAu+8DsC/ilFtEVHcH+wydcSpNE20AfSOduf6MkRFahL5FY7X1oU7nKVZFtfq8Fg==",
      "license": "MIT",
      "engines": {
        "node": ">=6"
      }
    },
    "node_modules/qs": {
      "version": "6.15.1",
      "resolved": "https://registry.npmjs.org/qs/-/qs-6.15.1.tgz",
      "integrity": "sha512-6YHEFRL9mfgcAvql/XhwTvf5jKcOiiupt2FiJxHkiX1z4j7WL8J/jRHYLluORvc1XxB5rV20KoeK00gVJamspg==",
      "license": "BSD-3-Clause",
      "dependencies": {
        "side-channel": "^1.1.0"
      },
      "engines": {
        "node": ">=0.6"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/range-parser": {
      "version": "1.2.1",
      "resolved": "https://registry.npmjs.org/range-parser/-/range-parser-1.2.1.tgz",
      "integrity": "sha512-Hrgsx+orqoygnmhFbKaHE6c296J+HTAQXoxEF6gNupROmmGJRoyzfG3ccAveqCBrwr/2yxQ5BVd/GTl5agOwSg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/raw-body": {
      "version": "3.0.2",
      "resolved": "https://registry.npmjs.org/raw-body/-/raw-body-3.0.2.tgz",
      "integrity": "sha512-K5zQjDllxWkf7Z5xJdV0/B0WTNqx6vxG70zJE4N0kBs4LovmEYWJzQGxC9bS9RAKu3bgM40lrd5zoLJ12MQ5BA==",
      "license": "MIT",
      "dependencies": {
        "bytes": "~3.1.2",
        "http-errors": "~2.0.1",
        "iconv-lite": "~0.7.0",
        "unpipe": "~1.0.0"
      },
      "engines": {
        "node": ">= 0.10"
      }
    },
    "node_modules/router": {
      "version": "2.2.0",
      "resolved": "https://registry.npmjs.org/router/-/router-2.2.0.tgz",
      "integrity": "sha512-nLTrUKm2UyiL7rlhapu/Zl45FwNgkZGaCpZbIHajDYgwlJCOzLSk+cIPAnsEqV955GjILJnKbdQC1nVPz+gAYQ==",
      "license": "MIT",
      "dependencies": {
        "debug": "^4.4.0",
        "depd": "^2.0.0",
        "is-promise": "^4.0.0",
        "parseurl": "^1.3.3",
        "path-to-regexp": "^8.0.0"
      },
      "engines": {
        "node": ">= 18"
      }
    },
    "node_modules/safe-buffer": {
      "version": "5.2.1",
      "resolved": "https://registry.npmjs.org/safe-buffer/-/safe-buffer-5.2.1.tgz",
      "integrity": "sha512-rp3So07KcdmmKbGvgaNxQSJr7bGVSVk5S9Eq1F+ppbRo70+YeaDxkw5Dd8NPN+GD6bjnYm2VuPuCXmpuYvmCXQ==",
      "funding": [
        {
          "type": "github",
          "url": "https://github.com/sponsors/feross"
        },
        {
          "type": "patreon",
          "url": "https://www.patreon.com/feross"
        },
        {
          "type": "consulting",
          "url": "https://feross.org/support"
        }
      ],
      "license": "MIT"
    },
    "node_modules/safer-buffer": {
      "version": "2.1.2",
      "resolved": "https://registry.npmjs.org/safer-buffer/-/safer-buffer-2.1.2.tgz",
      "integrity": "sha512-YZo3K82SD7Riyi0E1EQPojLz7kpepnSQI9IyPbHHg1XXXevb5dJI7tpyN2ADxGcQbHG7vcyRHk0cbwqcQriUtg==",
      "license": "MIT"
    },
    "node_modules/semver": {
      "version": "7.8.0",
      "resolved": "https://registry.npmjs.org/semver/-/semver-7.8.0.tgz",
      "integrity": "sha512-AcM7dV/5ul4EekoQ29Agm5vri8JNqRyj39o0qpX6vDF2GZrtutZl5RwgD1XnZjiTAfncsJhMI48QQH3sN87YNA==",
      "license": "ISC",
      "bin": {
        "semver": "bin/semver.js"
      },
      "engines": {
        "node": ">=10"
      }
    },
    "node_modules/send": {
      "version": "1.2.1",
      "resolved": "https://registry.npmjs.org/send/-/send-1.2.1.tgz",
      "integrity": "sha512-1gnZf7DFcoIcajTjTwjwuDjzuz4PPcY2StKPlsGAQ1+YH20IRVrBaXSWmdjowTJ6u8Rc01PoYOGHXfP1mYcZNQ==",
      "license": "MIT",
      "dependencies": {
        "debug": "^4.4.3",
        "encodeurl": "^2.0.0",
        "escape-html": "^1.0.3",
        "etag": "^1.8.1",
        "fresh": "^2.0.0",
        "http-errors": "^2.0.1",
        "mime-types": "^3.0.2",
        "ms": "^2.1.3",
        "on-finished": "^2.4.1",
        "range-parser": "^1.2.1",
        "statuses": "^2.0.2"
      },
      "engines": {
        "node": ">= 18"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/serve-static": {
      "version": "2.2.1",
      "resolved": "https://registry.npmjs.org/serve-static/-/serve-static-2.2.1.tgz",
      "integrity": "sha512-xRXBn0pPqQTVQiC8wyQrKs2MOlX24zQ0POGaj0kultvoOCstBQM5yvOhAVSUwOMjQtTvsPWoNCHfPGwaaQJhTw==",
      "license": "MIT",
      "dependencies": {
        "encodeurl": "^2.0.0",
        "escape-html": "^1.0.3",
        "parseurl": "^1.3.3",
        "send": "^1.2.0"
      },
      "engines": {
        "node": ">= 18"
      },
      "funding": {
        "type": "opencollective",
        "url": "https://opencollective.com/express"
      }
    },
    "node_modules/setprototypeof": {
      "version": "1.2.0",
      "resolved": "https://registry.npmjs.org/setprototypeof/-/setprototypeof-1.2.0.tgz",
      "integrity": "sha512-E5LDX7Wrp85Kil5bhZv46j8jOeboKq5JMmYM3gVGdGH8xFpPWXUMsNrlODCrkoxMEeNi/XZIwuRvY4XNwYMJpw==",
      "license": "ISC"
    },
    "node_modules/side-channel": {
      "version": "1.1.0",
      "resolved": "https://registry.npmjs.org/side-channel/-/side-channel-1.1.0.tgz",
      "integrity": "sha512-ZX99e6tRweoUXqR+VBrslhda51Nh5MTQwou5tnUDgbtyM0dBgmhEDtWGP/xbKn6hqfPRHujUNwz5fy/wbbhnpw==",
      "license": "MIT",
      "dependencies": {
        "es-errors": "^1.3.0",
        "object-inspect": "^1.13.3",
        "side-channel-list": "^1.0.0",
        "side-channel-map": "^1.0.1",
        "side-channel-weakmap": "^1.0.2"
      },
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/side-channel-list": {
      "version": "1.0.1",
      "resolved": "https://registry.npmjs.org/side-channel-list/-/side-channel-list-1.0.1.tgz",
      "integrity": "sha512-mjn/0bi/oUURjc5Xl7IaWi/OJJJumuoJFQJfDDyO46+hBWsfaVM65TBHq2eoZBhzl9EchxOijpkbRC8SVBQU0w==",
      "license": "MIT",
      "dependencies": {
        "es-errors": "^1.3.0",
        "object-inspect": "^1.13.4"
      },
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/side-channel-map": {
      "version": "1.0.1",
      "resolved": "https://registry.npmjs.org/side-channel-map/-/side-channel-map-1.0.1.tgz",
      "integrity": "sha512-VCjCNfgMsby3tTdo02nbjtM/ewra6jPHmpThenkTYh8pG9ucZ/1P8So4u4FGBek/BjpOVsDCMoLA/iuBKIFXRA==",
      "license": "MIT",
      "dependencies": {
        "call-bound": "^1.0.2",
        "es-errors": "^1.3.0",
        "get-intrinsic": "^1.2.5",
        "object-inspect": "^1.13.3"
      },
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/side-channel-weakmap": {
      "version": "1.0.2",
      "resolved": "https://registry.npmjs.org/side-channel-weakmap/-/side-channel-weakmap-1.0.2.tgz",
      "integrity": "sha512-WPS/HvHQTYnHisLo9McqBHOJk2FkHO/tlpvldyrnem4aeQp4hai3gythswg6p01oSoTl58rcpiFAjF2br2Ak2A==",
      "license": "MIT",
      "dependencies": {
        "call-bound": "^1.0.2",
        "es-errors": "^1.3.0",
        "get-intrinsic": "^1.2.5",
        "object-inspect": "^1.13.3",
        "side-channel-map": "^1.0.1"
      },
      "engines": {
        "node": ">= 0.4"
      },
      "funding": {
        "url": "https://github.com/sponsors/ljharb"
      }
    },
    "node_modules/sift": {
      "version": "17.1.3",
      "resolved": "https://registry.npmjs.org/sift/-/sift-17.1.3.tgz",
      "integrity": "sha512-Rtlj66/b0ICeFzYTuNvX/EF1igRbbnGSvEyT79McoZa/DeGhMyC5pWKOEsZKnpkqtSeovd5FL/bjHWC3CIIvCQ==",
      "license": "MIT"
    },
    "node_modules/socket.io": {
      "version": "4.8.3",
      "resolved": "https://registry.npmjs.org/socket.io/-/socket.io-4.8.3.tgz",
      "integrity": "sha512-2Dd78bqzzjE6KPkD5fHZmDAKRNe3J15q+YHDrIsy9WEkqttc7GY+kT9OBLSMaPbQaEd0x1BjcmtMtXkfpc+T5A==",
      "license": "MIT",
      "dependencies": {
        "accepts": "~1.3.4",
        "base64id": "~2.0.0",
        "cors": "~2.8.5",
        "debug": "~4.4.1",
        "engine.io": "~6.6.0",
        "socket.io-adapter": "~2.5.2",
        "socket.io-parser": "~4.2.4"
      },
      "engines": {
        "node": ">=10.2.0"
      }
    },
    "node_modules/socket.io-adapter": {
      "version": "2.5.6",
      "resolved": "https://registry.npmjs.org/socket.io-adapter/-/socket.io-adapter-2.5.6.tgz",
      "integrity": "sha512-DkkO/dz7MGln0dHn5bmN3pPy+JmywNICWrJqVWiVOyvXjWQFIv9c2h24JrQLLFJ2aQVQf/Cvl1vblnd4r2apLQ==",
      "license": "MIT",
      "dependencies": {
        "debug": "~4.4.1",
        "ws": "~8.18.3"
      }
    },
    "node_modules/socket.io-parser": {
      "version": "4.2.6",
      "resolved": "https://registry.npmjs.org/socket.io-parser/-/socket.io-parser-4.2.6.tgz",
      "integrity": "sha512-asJqbVBDsBCJx0pTqw3WfesSY0iRX+2xzWEWzrpcH7L6fLzrhyF8WPI8UaeM4YCuDfpwA/cgsdugMsmtz8EJeg==",
      "license": "MIT",
      "dependencies": {
        "@socket.io/component-emitter": "~3.1.0",
        "debug": "~4.4.1"
      },
      "engines": {
        "node": ">=10.0.0"
      }
    },
    "node_modules/socket.io/node_modules/accepts": {
      "version": "1.3.8",
      "resolved": "https://registry.npmjs.org/accepts/-/accepts-1.3.8.tgz",
      "integrity": "sha512-PYAthTa2m2VKxuvSD3DPC/Gy+U+sOA1LAuT8mkmRuvw+NACSaeXEQ+NHcVF7rONl6qcaxV3Uuemwawk+7+SJLw==",
      "license": "MIT",
      "dependencies": {
        "mime-types": "~2.1.34",
        "negotiator": "0.6.3"
      },
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/socket.io/node_modules/mime-db": {
      "version": "1.52.0",
      "resolved": "https://registry.npmjs.org/mime-db/-/mime-db-1.52.0.tgz",
      "integrity": "sha512-sPU4uV7dYlvtWJxwwxHD0PuihVNiE7TyAbQ5SWxDCB9mUYvOgroQOwYQQOKPJ8CIbE+1ETVlOoK1UC2nU3gYvg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/socket.io/node_modules/mime-types": {
      "version": "2.1.35",
      "resolved": "https://registry.npmjs.org/mime-types/-/mime-types-2.1.35.tgz",
      "integrity": "sha512-ZDY+bPm5zTTF+YpCrAU9nK0UgICYPT0QtT1NZWFv4s++TNkcgVaT0g6+4R2uI4MjQjzysHB1zxuWL50hzaeXiw==",
      "license": "MIT",
      "dependencies": {
        "mime-db": "1.52.0"
      },
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/socket.io/node_modules/negotiator": {
      "version": "0.6.3",
      "resolved": "https://registry.npmjs.org/negotiator/-/negotiator-0.6.3.tgz",
      "integrity": "sha512-+EUsqGPLsM+j/zdChZjsnX51g4XrHFOIXwfnCVPGlQk/k5giakcKsuxCObBRu6DSm9opw/O6slWbJdghQM4bBg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/sparse-bitfield": {
      "version": "3.0.3",
      "resolved": "https://registry.npmjs.org/sparse-bitfield/-/sparse-bitfield-3.0.3.tgz",
      "integrity": "sha512-kvzhi7vqKTfkh0PZU+2D2PIllw2ymqJKujUcyPMd9Y75Nv4nPbGJZXNhxsgdQab2BmlDct1YnfQCguEvHr7VsQ==",
      "license": "MIT",
      "dependencies": {
        "memory-pager": "^1.0.2"
      }
    },
    "node_modules/statuses": {
      "version": "2.0.2",
      "resolved": "https://registry.npmjs.org/statuses/-/statuses-2.0.2.tgz",
      "integrity": "sha512-DvEy55V3DB7uknRo+4iOGT5fP1slR8wQohVdknigZPMpMstaKJQWhwiYBACJE3Ul2pTnATihhBYnRhZQHGBiRw==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/toidentifier": {
      "version": "1.0.1",
      "resolved": "https://registry.npmjs.org/toidentifier/-/toidentifier-1.0.1.tgz",
      "integrity": "sha512-o5sSPKEkg/DIQNmH43V0/uerLrpzVedkUh8tGNvaeXpfpuwjKenlSox/2O/BTlZUtEe+JG7s5YhEz608PlAHRA==",
      "license": "MIT",
      "engines": {
        "node": ">=0.6"
      }
    },
    "node_modules/tr46": {
      "version": "5.1.1",
      "resolved": "https://registry.npmjs.org/tr46/-/tr46-5.1.1.tgz",
      "integrity": "sha512-hdF5ZgjTqgAntKkklYw0R03MG2x/bSzTtkxmIRw/sTNV8YXsCJ1tfLAX23lhxhHJlEf3CRCOCGGWw3vI3GaSPw==",
      "license": "MIT",
      "dependencies": {
        "punycode": "^2.3.1"
      },
      "engines": {
        "node": ">=18"
      }
    },
    "node_modules/type-is": {
      "version": "2.0.1",
      "resolved": "https://registry.npmjs.org/type-is/-/type-is-2.0.1.tgz",
      "integrity": "sha512-OZs6gsjF4vMp32qrCbiVSkrFmXtG/AZhY3t0iAMrMBiAZyV9oALtXO8hsrHbMXF9x6L3grlFuwW2oAz7cav+Gw==",
      "license": "MIT",
      "dependencies": {
        "content-type": "^1.0.5",
        "media-typer": "^1.1.0",
        "mime-types": "^3.0.0"
      },
      "engines": {
        "node": ">= 0.6"
      }
    },
    "node_modules/undici-types": {
      "version": "7.19.2",
      "resolved": "https://registry.npmjs.org/undici-types/-/undici-types-7.19.2.tgz",
      "integrity": "sha512-qYVnV5OEm2AW8cJMCpdV20CDyaN3g0AjDlOGf1OW4iaDEx8MwdtChUp4zu4H0VP3nDRF/8RKWH+IPp9uW0YGZg==",
      "license": "MIT"
    },
    "node_modules/unpipe": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/unpipe/-/unpipe-1.0.0.tgz",
      "integrity": "sha512-pjy2bYhSsufwWlKwPc+l3cN7+wuJlK6uz0YdJEOlQDbl6jo/YlPi4mb8agUkVC8BF7V8NuzeyPNqRksA3hztKQ==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/vary": {
      "version": "1.1.2",
      "resolved": "https://registry.npmjs.org/vary/-/vary-1.1.2.tgz",
      "integrity": "sha512-BNGbWLfd0eUPabhkXUVm0j8uuvREyTh5ovRa/dyow/BqAbZJyC+5fU+IzQOzmAKzYqYRAISoRhdQr3eIZ/PXqg==",
      "license": "MIT",
      "engines": {
        "node": ">= 0.8"
      }
    },
    "node_modules/webidl-conversions": {
      "version": "7.0.0",
      "resolved": "https://registry.npmjs.org/webidl-conversions/-/webidl-conversions-7.0.0.tgz",
      "integrity": "sha512-VwddBukDzu71offAQR975unBIGqfKZpM+8ZX6ySk8nYhVoo5CYaZyzt3YBvYtRtO+aoGlqxPg/B87NGVZ/fu6g==",
      "license": "BSD-2-Clause",
      "engines": {
        "node": ">=12"
      }
    },
    "node_modules/whatwg-url": {
      "version": "14.2.0",
      "resolved": "https://registry.npmjs.org/whatwg-url/-/whatwg-url-14.2.0.tgz",
      "integrity": "sha512-De72GdQZzNTUBBChsXueQUnPKDkg/5A5zp7pFDuQAj5UFoENpiACU0wlCvzpAGnTkj++ihpKwKyYewn/XNUbKw==",
      "license": "MIT",
      "dependencies": {
        "tr46": "^5.1.0",
        "webidl-conversions": "^7.0.0"
      },
      "engines": {
        "node": ">=18"
      }
    },
    "node_modules/wrappy": {
      "version": "1.0.2",
      "resolved": "https://registry.npmjs.org/wrappy/-/wrappy-1.0.2.tgz",
      "integrity": "sha512-l4Sp/DRseor9wL6EvV2+TuQn63dMkPjZ/sp9XkghTEbV9KlPS1xUsZ3u7/IQO4wxtcFB4bgpQPRcR3QCvezPcQ==",
      "license": "ISC"
    },
    "node_modules/ws": {
      "version": "8.18.3",
      "resolved": "https://registry.npmjs.org/ws/-/ws-8.18.3.tgz",
      "integrity": "sha512-PEIGCY5tSlUt50cqyMXfCzX+oOPqN0vuGqWzbcJ2xvnkzkq46oOpz7dQaTDBdfICb4N14+GARUDw2XV2N4tvzg==",
      "license": "MIT",
      "engines": {
        "node": ">=10.0.0"
      },
      "peerDependencies": {
        "bufferutil": "^4.0.1",
        "utf-8-validate": ">=5.0.2"
      },
      "peerDependenciesMeta": {
        "bufferutil": {
          "optional": true
        },
        "utf-8-validate": {
          "optional": true
        }
      }
    },
    "node_modules/zod": {
      "version": "4.4.3",
      "resolved": "https://registry.npmjs.org/zod/-/zod-4.4.3.tgz",
      "integrity": "sha512-ytENFjIJFl2UwYglde2jchW2Hwm4GJFLDiSXWdTrJQBIN9Fcyp7n4DhxJEiWNAJMV1/BqWfW/kkg71UDcHJyTQ==",
      "license": "MIT",
      "funding": {
        "url": "https://github.com/sponsors/colinhacks"
      }
    }
  }
}

