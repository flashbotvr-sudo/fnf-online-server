// FNF Online matchmaking + relay server (HTTP version)
// Works on normal web hosts like Render, and locally for testing.
// No npm packages needed. Run: node server.js   (PORT env var to change port, default 7777)
//
// The game sends POST /poll with {sid, out:[messages]} and gets back {sid, msgs:[messages]}.
// Open the server address in a browser to check if its up.

const http = require("http");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 7777);
const PROTOCOL = 4;
const SESSION_TIMEOUT_MS = 90000; // no poll for this long = player is gone (long so installing a big mod doesnt kick u)
const SONGS = ["bopeebo", "fresh", "dadbattle"]; // songs matches can pick, change whenever

const sessions = new Map(); // sid -> client
let queue = []; // sids waiting for a match
const rooms = new Map(); // roomId -> { a, b, song }
let shuttingDown = false;

// ------------------------------------------------------------ accounts
// Saved in Upstash Redis when UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN are set (Render > Environment).
// Without them accounts live in memory and get wiped when the server restarts.
const DB_URL = (process.env.UPSTASH_REDIS_REST_URL || "").replace(/\/+$/, "");
const DB_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || "";
const memoryAccounts = new Map();
if (!DB_URL || !DB_TOKEN) console.log("WARNING: no database set, accounts will be lost on restart");

async function db(command) {
  const r = await fetch(DB_URL, {
    method: "POST",
    headers: { Authorization: "Bearer " + DB_TOKEN, "Content-Type": "application/json" },
    body: JSON.stringify(command),
  });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}

async function getAccount(key) {
  if (!DB_URL || !DB_TOKEN) return memoryAccounts.get(key) || null;
  const v = await db(["GET", "acct:" + key]);
  return v ? JSON.parse(v) : null;
}

async function createAccount(key, acct) {
  if (!DB_URL || !DB_TOKEN) {
    if (memoryAccounts.has(key)) return false;
    memoryAccounts.set(key, acct);
    return true;
  }
  // NX = only if nobody took the name a millisecond ago
  return (await db(["SET", "acct:" + key, JSON.stringify(acct), "NX"])) === "OK";
}

// ------------------------------------------------------------ owner + bans
// accounts in OWNERS (Render > Environment, comma separated) can use owner tools. default: Owner
const OWNERS = (process.env.OWNERS || "Owner").split(",").map((n) => n.trim().toLowerCase()).filter(Boolean);
const memoryBans = new Set();

function isOwnerName(name) {
  return OWNERS.includes(String(name || "").toLowerCase());
}

async function isBanned(name) {
  const key = String(name || "").toLowerCase();
  if (!DB_URL || !DB_TOKEN) return memoryBans.has(key);
  return (await db(["SISMEMBER", "bans", key])) === 1;
}

async function setBanned(name, banned) {
  const key = String(name || "").toLowerCase();
  if (!DB_URL || !DB_TOKEN) {
    if (banned) memoryBans.add(key);
    else memoryBans.delete(key);
    return;
  }
  await db([banned ? "SADD" : "SREM", "bans", key]);
}

async function listBans() {
  if (!DB_URL || !DB_TOKEN) return [...memoryBans];
  return (await db(["SMEMBERS", "bans"])) || [];
}

function hashPassword(password, salt) {
  return crypto.scryptSync(password, salt, 32).toString("hex");
}
function sha(text) {
  return crypto.createHash("sha256").update(text).digest("hex");
}
function sameHex(a, b) {
  const x = Buffer.from(String(a), "hex");
  const y = Buffer.from(String(b), "hex");
  return x.length === y.length && x.length > 0 && crypto.timingSafeEqual(x, y);
}

function validName(n) {
  return typeof n === "string" && n.length >= 3 && n.length <= 16 && /^[\x21-\x7E]+$/.test(n);
}
function validPassword(p) {
  return typeof p === "string" && p.length >= 6 && p.length <= 64 && /^[\x21-\x7E]+$/.test(p);
}

// slow down password guessing: per ip and per account
const attempts = new Map(); // key -> {count, until}
function tooMany(key, max, windowMs) {
  const now = Date.now();
  let a = attempts.get(key);
  if (!a || a.until < now) a = { count: 0, until: now + windowMs };
  a.count++;
  attempts.set(key, a);
  return a.count > max;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, a] of attempts) if (a.until < now) attempts.delete(k);
}, 60000);

async function onAuth(body, ip) {
  if (shuttingDown) return { ok: false, error: "Server Is Down. Be Back Soon!" };
  if (body.protocol !== PROTOCOL) return { ok: false, error: "outdated mod, download the new version" };
  const name = body.name;
  const password = body.password;
  if (!validName(name)) return { ok: false, error: "name must be 3-16 characters, no spaces" };
  if (!validPassword(password)) return { ok: false, error: "password must be 6-64 characters, no spaces" };
  if (tooMany("ip:" + ip, 20, 60000)) return { ok: false, error: "too many tries, wait a minute" };

  const key = name.toLowerCase();
  let acct = await getAccount(key);
  if (!acct) {
    // new name = make the account
    const salt = crypto.randomBytes(16).toString("hex");
    const token = crypto.randomBytes(32).toString("hex");
    acct = { name, salt, hash: hashPassword(password, salt), tokenHash: sha(token), created: Date.now() };
    if (!(await createAccount(key, acct))) return { ok: false, error: "that name just got taken, try again" };
    console.log(`new account: ${name}`);
    return { ok: true, created: true, name, token };
  }

  if (tooMany("acct:" + key, 8, 5 * 60000)) return { ok: false, error: "too many wrong passwords, wait 5 minutes" };
  if (await isBanned(key)) return { ok: false, error: "this account is banned" };
  if (!sameHex(hashPassword(password, acct.salt), acct.hash)) return { ok: false, error: "wrong password (or that name is taken)" };
  attempts.delete("acct:" + key);
  // the same token works on all ur devices, so pc and phone can both stay logged in
  const token = crypto.randomBytes(32).toString("hex");
  if (!acct.tokens) acct.tokens = [acct.tokenHash];
  acct.tokens = [sha(token), ...acct.tokens].slice(0, 5); // remember the last 5 devices
  acct.tokenHash = acct.tokens[0];
  if (DB_URL && DB_TOKEN) await db(["SET", "acct:" + key, JSON.stringify(acct)]);
  else memoryAccounts.set(key, acct);
  return { ok: true, created: false, name: acct.name, token };
}

async function checkToken(name, token) {
  if (!validName(name) || typeof token !== "string" || token.length < 10) return null;
  const acct = await getAccount(name.toLowerCase());
  if (!acct) return null;
  const list = acct.tokens || [acct.tokenHash];
  const h = sha(token);
  return list.some((t) => sameHex(t, h)) ? acct.name : null;
}

function newId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

function send(c, msg) {
  if (c) c.inbox.push(msg);
}

function opponentOf(c) {
  const room = rooms.get(c.room);
  if (!room) return null;
  return sessions.get(room.a === c.sid ? room.b : room.a) || null;
}

function leaveQueue(c) {
  queue = queue.filter((id) => id !== c.sid);
}

function leaveRoom(c) {
  if (!c.room) return;
  const opp = opponentOf(c);
  rooms.delete(c.room);
  c.room = null;
  if (opp) {
    opp.room = null;
    send(opp, { t: "opponent_left" });
  }
}

function broadcastQueueCount() {
  for (const id of queue) send(sessions.get(id), { t: "queued", count: queue.length });
}

function makeMatch(a, b, isPrivate) {
  const roomId = "r" + newId();
  const song = SONGS[Math.floor(Math.random() * SONGS.length)];
  rooms.set(roomId, { a: a.sid, b: b.sid, song });
  a.room = b.room = roomId;
  // a plays as bf (picks the song), b as dad
  send(a, { t: "match", room: roomId, song, side: "bf", opponent: b.name, private: !!isPrivate });
  send(b, { t: "match", room: roomId, song, side: "dad", opponent: a.name, private: !!isPrivate });
  console.log(`match${isPrivate ? " (private)" : ""}: ${a.name} vs ${b.name}`);
}

function tryMatch() {
  while (queue.length >= 2) {
    const a = sessions.get(queue.shift());
    const b = sessions.get(queue.shift());
    if (!a || !b) continue;
    makeMatch(a, b, false);
  }
  broadcastQueueCount();
}

// private rooms: code -> host sid
const privateRooms = new Map();
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // no 0/O or 1/I mixups

function closeHostedRoom(c) {
  if (c.hostCode) {
    privateRooms.delete(c.hostCode);
    c.hostCode = null;
  }
}

function newRoomCode() {
  for (let tries = 0; tries < 50; tries++) {
    let code = "";
    for (let i = 0; i < 5; i++) code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!privateRooms.has(code)) return code;
  }
  return null;
}

function kickSession(target, msg) {
  leaveQueue(target);
  leaveRoom(target);
  closeHostedRoom(target);
  target.kickMsg = msg; // told on their next poll, then removed
}

function cleanName(n) {
  // same rules as the game: 3-16 letters, numbers or symbols, no spaces
  n = String(n || "").replace(/[^\x21-\x7E]/g, "").slice(0, 16);
  return n.length >= 3 ? n : "guest" + Math.floor(Math.random() * 9999);
}

async function handle(c, msg) {
  if (!msg || typeof msg !== "object") return;
  switch (msg.t) {
    case "queue":
      leaveRoom(c);
      closeHostedRoom(c);
      if (!queue.includes(c.sid)) queue.push(c.sid);
      tryMatch();
      break;
    case "cancel":
      leaveQueue(c);
      closeHostedRoom(c);
      broadcastQueueCount();
      break;
    case "create_room": {
      leaveQueue(c);
      leaveRoom(c);
      closeHostedRoom(c);
      const code = newRoomCode();
      if (!code) return send(c, { t: "error", code: "room", msg: "couldnt make a room, try again" });
      privateRooms.set(code, c.sid);
      c.hostCode = code;
      send(c, { t: "room_created", code });
      break;
    }
    case "join_room": {
      const code = String(msg.code || "").toUpperCase().trim();
      const host = sessions.get(privateRooms.get(code));
      if (!host || host === c || host.kickMsg) return send(c, { t: "error", code: "room", msg: "room not found, check the code" });
      leaveQueue(c);
      leaveRoom(c);
      closeHostedRoom(c);
      closeHostedRoom(host);
      makeMatch(host, c, true);
      break;
    }
    // ---------- owner tools (server checks the account, the mod alone cant do anything)
    case "admin_list": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const players = [...sessions.values()]
        .filter((s) => !s.kickMsg)
        .map((s) => ({ name: s.name, status: s.room ? "in a match" : queue.includes(s.sid) ? "searching" : s.hostCode ? "private room" : "menu" }));
      send(c, { t: "admin_list", players, banned: await listBans() });
      break;
    }
    case "admin_kick":
    case "admin_ban":
    case "admin_unban": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const name = String(msg.name || "");
      if (!name || isOwnerName(name)) return send(c, { t: "admin_done", msg: "cant do that to an owner" });
      if (msg.t === "admin_unban") {
        await setBanned(name, false);
        console.log(`owner ${c.name} unbanned ${name}`);
        return send(c, { t: "admin_done", msg: name + " unbanned" });
      }
      if (msg.t === "admin_ban") await setBanned(name, true);
      for (const s of sessions.values()) {
        if (s.name.toLowerCase() === name.toLowerCase()) kickSession(s, msg.t === "admin_ban" ? "u got banned by the owner" : "u got kicked by the owner");
      }
      console.log(`owner ${c.name} ${msg.t === "admin_ban" ? "banned" : "kicked"} ${name}`);
      send(c, { t: "admin_done", msg: name + (msg.t === "admin_ban" ? " banned" : " kicked") });
      break;
    }
    case "leave":
      leaveRoom(c);
      break;
    case "relay":
      // gameplay stuff (ready, score, done) goes straight to the opponent
      send(opponentOf(c), { t: "relay", data: msg.data });
      break;
    case "bye":
      removeSession(c);
      break;
    default:
      break;
  }
}

function removeSession(c) {
  if (!sessions.has(c.sid)) return;
  leaveQueue(c);
  leaveRoom(c);
  closeHostedRoom(c);
  sessions.delete(c.sid);
  broadcastQueueCount();
}

async function onPoll(body) {
  if (shuttingDown) return { msgs: [{ t: "server_down" }] };

  const out = Array.isArray(body.out) ? body.out.slice(0, 50) : [];
  let c = body.sid ? sessions.get(body.sid) : null;

  if (!c) {
    // new player, first message has to be hello
    const hello = out.find((m) => m && m.t === "hello");
    if (!hello) return { error: "bad_session", msgs: [{ t: "error", msg: "session expired" }] };
    if (hello.protocol !== PROTOCOL) return { msgs: [{ t: "error", msg: "outdated mod, download the new version" }] };
    const realName = await checkToken(hello.name, hello.token);
    if (!realName) return { msgs: [{ t: "error", code: "auth", msg: "log in again" }] };
    if (await isBanned(realName)) return { msgs: [{ t: "error", code: "banned", msg: "this account is banned" }] };
    // same account logging in twice: kick the old one
    for (const old of [...sessions.values()]) if (old.name.toLowerCase() === realName.toLowerCase()) removeSession(old);
    c = { sid: newId(), name: realName, room: null, inbox: [], lastSeen: Date.now() };
    sessions.set(c.sid, c);
    send(c, { t: "welcome", name: c.name, online: sessions.size, owner: isOwnerName(c.name) });
  }

  c.lastSeen = Date.now();
  if (c.kickMsg) {
    // kicked/banned: tell them once, then they're gone
    const msg = c.kickMsg;
    sessions.delete(c.sid);
    return { msgs: [{ t: "kicked", msg }] };
  }
  for (const m of out) if (m && m.t !== "hello") await handle(c, m);
  if (!sessions.has(c.sid)) return { msgs: [] }; // said bye

  return { sid: c.sid, msgs: c.inbox.splice(0) };
}

const server = http.createServer((req, res) => {
  const isPoll = req.url.startsWith("/poll");
  const isAuth = req.url.startsWith("/auth");
  if (req.method === "POST" && (isPoll || isAuth)) {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 65536) req.destroy(); // junk protection
    });
    req.on("end", async () => {
      let body = {};
      try {
        body = JSON.parse(data || "{}");
      } catch {
        body = {};
      }
      const ip = String(req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();
      let result;
      try {
        result = isAuth ? await onAuth(body, ip) : await onPoll(body);
      } catch (e) {
        console.log("error:", e.message);
        result = isAuth ? { ok: false, error: "server error, try again" } : { msgs: [] };
      }
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify(result));
    });
    return;
  }

  // anything else: simple status page, handy to check in a browser
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`FNF Online server is running. players online: ${sessions.size}, in queue: ${queue.length}, matches: ${rooms.size}, accounts saved: ${DB_URL && DB_TOKEN ? "yes" : "NO (database not set)"}\n`);
});

// players who stopped polling (closed the game, lost wifi) get removed
setInterval(() => {
  const now = Date.now();
  for (const c of [...sessions.values()]) {
    if (now - c.lastSeen > SESSION_TIMEOUT_MS) removeSession(c);
  }
}, 5000);

server.listen(PORT, () => {
  console.log(`fnf online server on port ${PORT}`);
  console.log("");
  console.log(`same pc:   http://127.0.0.1:${PORT}`);
  const nets = require("os").networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const n of nets[name] || []) {
      if (n.family === "IPv4" && !n.internal) console.log(`same wifi (${name}): http://${n.address}:${PORT}`);
    }
  }
  console.log("");
});

server.on("error", (e) => {
  if (e.code === "EADDRINUSE") {
    console.log(`port ${PORT} is already being used. another server window is probably still open, close it first.`);
  } else {
    console.log("server error:", e.message);
  }
  process.exitCode = 1;
});

// closing the window / ctrl+c / host restarting: tell players for a few seconds, then stop
function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log("shutting down, telling players...");
  setTimeout(() => process.exit(0), 3000);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.on("SIGHUP", shutdown);
if (process.platform === "win32") process.on("SIGBREAK", shutdown);
