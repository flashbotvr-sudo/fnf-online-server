// FNF Online matchmaking + relay server (HTTP version)
// Works on normal web hosts like Render, and locally for testing.
// No npm packages needed. Run: node server.js   (PORT env var to change port, default 7777)
//
// The game sends POST /poll with {sid, out:[messages]} and gets back {sid, msgs:[messages]}.
// Open the server address in a browser to check if its up.

const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { spawn } = require("child_process");
const { Readable } = require("stream");
const { pipeline } = require("stream/promises");

const PORT = Number(process.env.PORT || 7777);
const PROTOCOL = 7;
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

// every account name (lowercase -> how they typed it), kept in memory so the owner list is free
const allAccounts = new Map();

async function loadAllAccounts() {
  if (!DB_URL || !DB_TOKEN) return;
  try {
    let cursor = "0";
    const keys = [];
    do {
      const [next, batch] = await db(["SCAN", cursor, "MATCH", "acct:*", "COUNT", "500"]);
      cursor = String(next);
      keys.push(...batch);
    } while (cursor !== "0");
    for (let i = 0; i < keys.length; i += 100) {
      const chunk = keys.slice(i, i + 100);
      const vals = await db(["MGET", ...chunk]);
      vals.forEach((v, j) => {
        const lower = chunk[j].slice(5);
        let display = lower;
        try {
          display = JSON.parse(v).name || lower;
        } catch {}
        allAccounts.set(lower, display);
      });
    }
    console.log(`loaded ${allAccounts.size} accounts`);
  } catch (e) {
    console.log("couldnt load account list:", e.message);
  }
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
  if (body.protocol !== PROTOCOL) return { ok: false, error: "ur mod is outdated! get the new version from GameBanana" };
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
    allAccounts.set(key, name);
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

// ------------------------------------------------------------ mod packer
// The game can only unzip plain .zip files. The server downloads the mod from GameBanana
// (zip, rar or 7z), unpacks it with 7-Zip, finds the V-Slice mod folder and repacks just that as a clean zip.
const SEVEN_ZIP = process.env.SEVEN_ZIP || path.join(__dirname, "7zz");
const MOD_DIR = path.join(os.tmpdir(), "fnf-mods");
const MAX_MOD_BYTES = 1.5 * 1024 * 1024 * 1024;
const modJobs = new Map(); // id -> {state, pct, msg, file, size, touched}

function allowedModUrl(u) {
  if (process.env.ALLOW_ANY_MOD_URL === "1") return true; // local testing only
  try {
    const url = new URL(u);
    return url.protocol === "https:" && (url.hostname === "gamebanana.com" || url.hostname.endsWith(".gamebanana.com"));
  } catch {
    return false;
  }
}

function run7z(args, cwd) {
  return new Promise((resolve, reject) => {
    try {
      fs.chmodSync(SEVEN_ZIP, 0o755);
    } catch {}
    const p = spawn(SEVEN_ZIP, args, { cwd, stdio: ["ignore", "ignore", "pipe"] });
    let err = "";
    p.stderr.on("data", (d) => (err += d).slice(-2000));
    p.on("error", (e) => reject(new Error("7-Zip missing on the server (" + e.message + ")")));
    p.on("close", (code) => (code === 0 || code === 1 ? resolve() : reject(new Error("unpacking failed: " + err.slice(-200)))));
  });
}

function findModRoot(dir, depth = 0) {
  // shallowest folder that has _polymod_meta.json
  if (fs.existsSync(path.join(dir, "_polymod_meta.json"))) return dir;
  if (depth > 5) return null;
  let found = null;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.name === "__MACOSX") continue;
    const r = findModRoot(path.join(dir, entry.name), depth + 1);
    if (r && (!found || r.length < found.length)) found = r;
  }
  return found;
}

function cleanupOldMods() {
  // keep only the newest few finished mods on disk
  const done = [...modJobs.entries()].filter(([, j]) => j.state === "ready").sort((a, b) => b[1].touched - a[1].touched);
  for (const [id, j] of done.slice(3)) {
    try {
      fs.rmSync(path.dirname(j.file), { recursive: true, force: true });
    } catch {}
    modJobs.delete(id);
  }
}

async function prepareMod(id, url) {
  const job = modJobs.get(id);
  const work = path.join(MOD_DIR, id);
  fs.rmSync(work, { recursive: true, force: true });
  fs.mkdirSync(work, { recursive: true });
  try {
    // 1) download from GameBanana
    job.state = "downloading";
    const res = await fetch(url, { redirect: "follow", headers: { "User-Agent": "FNF-Online-Server" } });
    if (!res.ok) throw new Error("GameBanana said " + res.status);
    const total = Number(res.headers.get("content-length") || 0);
    if (total > MAX_MOD_BYTES) throw new Error("mod is too big for the server (" + Math.round(total / 1048576) + " MB)");
    const nameFromUrl = decodeURIComponent(new URL(res.url).pathname.split("/").pop() || "mod.bin");
    const ext = (path.extname(nameFromUrl) || ".bin").toLowerCase();
    const src = path.join(work, "source" + ext);
    let got = 0;
    const counter = new (require("stream").Transform)({
      transform(chunk, _enc, cb) {
        got += chunk.length;
        if (got > MAX_MOD_BYTES) return cb(new Error("mod is too big for the server"));
        if (total) job.pct = Math.floor((got / total) * 100);
        cb(null, chunk);
      },
    });
    await pipeline(Readable.fromWeb(res.body), counter, fs.createWriteStream(src));

    // 2) unpack (zip, rar, 7z, anything 7-Zip knows)
    job.state = "unpacking";
    job.pct = 0;
    const out = path.join(work, "x");
    fs.mkdirSync(out);
    await run7z(["x", src, "-o" + out, "-y", "-bso0", "-bsp0"], work);
    fs.rmSync(src, { force: true });

    // 3) find the actual mod folder
    const root = findModRoot(out);
    if (!root) throw new Error("thats not a V-Slice mod (no _polymod_meta.json inside)");

    // 4) repack as a plain zip the game can read
    job.state = "packing";
    const zip = path.join(work, "mod.zip");
    await run7z(["a", "-tzip", "-mx=1", "-bso0", "-bsp0", zip, "."], root);
    fs.rmSync(out, { recursive: true, force: true });

    job.file = zip;
    job.size = fs.statSync(zip).size;
    job.state = "ready";
    job.pct = 100;
    job.touched = Date.now();
    console.log(`mod ready: ${url} (${Math.round(job.size / 1048576)} MB)`);
    cleanupOldMods();
  } catch (e) {
    job.state = "error";
    job.msg = e.message;
    console.log(`mod failed: ${url}: ${e.message}`);
    fs.rmSync(work, { recursive: true, force: true });
  }
}

// ---------- player to player mod transfer
// the picker's game zips their mod folder and uploads it in small base64 chunks,
// the other player downloads the finished zip from /modfile
const MAX_UPLOAD_BYTES = 700 * 1024 * 1024;

function safeJobId(id) {
  return /^[A-Za-z0-9_.-]{3,120}$/.test(String(id || ""));
}

function onModChunk(body) {
  const id = "up-" + String(body.id || "");
  if (!safeJobId(id)) return { ok: false, error: "bad id" };
  const i = Number(body.i);
  const n = Number(body.n);
  if (!Number.isInteger(i) || !Number.isInteger(n) || i < 0 || n < 1 || i >= n || n > 1000) return { ok: false, error: "bad chunk" };
  const data = Buffer.from(String(body.data || ""), "base64");
  const dir = path.join(MOD_DIR, id);
  const file = path.join(dir, "mod.zip");
  let job = modJobs.get(id);
  if (i === 0) {
    // (re)start the upload
    fs.rmSync(dir, { recursive: true, force: true });
    fs.mkdirSync(dir, { recursive: true });
    job = { state: "uploading", pct: 0, msg: "", file, size: 0, next: 0, touched: Date.now() };
    modJobs.set(id, job);
  }
  if (!job || job.state !== "uploading") return { ok: false, error: "upload not started" };
  if (i !== job.next) return { ok: false, error: "chunk out of order", expected: job.next };
  if (job.size + data.length > MAX_UPLOAD_BYTES) {
    job.state = "error";
    job.msg = "mod is too big to send (over 700 MB)";
    fs.rmSync(dir, { recursive: true, force: true });
    return { ok: false, error: job.msg };
  }
  fs.appendFileSync(file, data);
  job.size += data.length;
  job.next = i + 1;
  job.pct = Math.floor(((i + 1) / n) * 100);
  job.touched = Date.now();
  if (job.next === n) {
    job.state = "ready";
    console.log(`mod uploaded: ${id} (${Math.round(job.size / 1048576)} MB)`);
    cleanupOldMods();
  }
  return { ok: true, next: job.next, state: job.state };
}

function startModJob(url) {
  const id = crypto.createHash("sha1").update(url).digest("hex").slice(0, 16);
  const existing = modJobs.get(id);
  if (existing && existing.state !== "error") {
    existing.touched = Date.now();
    return id;
  }
  modJobs.set(id, { state: "queued", pct: 0, msg: "", file: null, size: 0, touched: Date.now() });
  prepareMod(id, url);
  return id;
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

// private rooms: code -> { name (host account), made }
// kept by account name so the host can drop out for a bit (game paused, phone in background) and come back
const privateRooms = new Map();
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ"; // letters only (game font has no numbers), no I/O mixups
const ROOM_LIFE_MS = 20 * 60 * 1000;
const HOST_TIMEOUT_MS = 10 * 60 * 1000; // hosts waiting for a friend stay longer

function closeHostedRoom(c) {
  if (c.hostCode) {
    privateRooms.delete(c.hostCode);
    c.hostCode = null;
  }
}

function findSessionByName(name) {
  const low = String(name).toLowerCase();
  for (const s of sessions.values()) if (!s.kickMsg && s.name.toLowerCase() === low) return s;
  return null;
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
      // reconnecting host asks for the same code back
      const want = String(msg.code || "").toUpperCase().trim();
      const old = want ? privateRooms.get(want) : null;
      if (old && old.name.toLowerCase() === c.name.toLowerCase()) {
        if (c.hostCode && c.hostCode !== want) closeHostedRoom(c);
        old.made = Date.now();
        c.hostCode = want;
        return send(c, { t: "room_created", code: want });
      }
      closeHostedRoom(c);
      const code = newRoomCode();
      if (!code) return send(c, { t: "error", code: "room", msg: "couldnt make a room, try again" });
      privateRooms.set(code, { name: c.name, made: Date.now() });
      c.hostCode = code;
      console.log(`room ${code} made by ${c.name}`);
      send(c, { t: "room_created", code });
      break;
    }
    case "join_room": {
      const code = String(msg.code || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
      const entry = privateRooms.get(code);
      if (!entry) {
        console.log(`join ${code} by ${c.name}: no such room (rooms: ${[...privateRooms.keys()].join(",") || "none"})`);
        return send(c, { t: "error", code: "room", msg: "room " + code + " not found, check the code" });
      }
      if (entry.name.toLowerCase() === c.name.toLowerCase())
        return send(c, { t: "error", code: "room", msg: "thats ur own room! ur friend has to join\n(2 devices need 2 different accounts)" });
      const host = findSessionByName(entry.name);
      if (!host || host.hostCode !== code || host.room)
        return send(c, { t: "error", code: "room", msg: entry.name + " isnt in the room rn.\ntell them to keep FNF open on the room screen" });
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
      // memory mode: accounts only live in memoryAccounts
      if (!DB_URL || !DB_TOKEN) for (const [k, a] of memoryAccounts) allAccounts.set(k, a.name);
      send(c, { t: "admin_list", players, accounts: [...allAccounts.values()], banned: await listBans() });
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
    if (hello.protocol !== PROTOCOL) return { msgs: [{ t: "error", msg: "ur mod is outdated! get the new version from GameBanana" }] };
    const realName = await checkToken(hello.name, hello.token);
    if (!realName) return { msgs: [{ t: "error", code: "auth", msg: "log in again" }] };
    if (await isBanned(realName)) return { msgs: [{ t: "error", code: "banned", msg: "this account is banned" }] };
    // same account logging in twice: kick the old one
    for (const old of [...sessions.values()])
      if (old.name.toLowerCase() === realName.toLowerCase()) {
        old.hostCode = null; // room stays, the new login can take it back
        leaveQueue(old);
        leaveRoom(old);
        old.kickMsg = "ur account logged in on another device";
      }
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

  // ---------- mod upload from a player
  if (req.method === "POST" && req.url.startsWith("/modchunk")) {
    const chunks = [];
    let len = 0;
    req.on("data", (chunk) => {
      len += chunk.length;
      if (len > 6 * 1024 * 1024) return req.destroy(); // one chunk max ~4MB of base64
      chunks.push(chunk);
    });
    req.on("end", () => {
      let body = {};
      try {
        body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
      } catch {}
      let result;
      try {
        result = onModChunk(body);
      } catch (e) {
        result = { ok: false, error: "server error: " + e.message };
      }
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify(result));
    });
    return;
  }

  // ---------- mod packer endpoints
  if (req.method === "POST" && req.url.startsWith("/modprep")) {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 4096) req.destroy();
    });
    req.on("end", () => {
      let body = {};
      try {
        body = JSON.parse(data || "{}");
      } catch {}
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      if (!allowedModUrl(body.url)) return res.end(JSON.stringify({ ok: false, error: "only GameBanana downloads are allowed" }));
      res.end(JSON.stringify({ ok: true, job: startModJob(body.url) }));
    });
    return;
  }
  if (req.method === "GET" && req.url.startsWith("/modstatus")) {
    const id = new URL(req.url, "http://x").searchParams.get("job");
    const j = modJobs.get(id);
    if (j) j.touched = Date.now();
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    res.end(JSON.stringify(j ? { state: j.state, pct: j.pct, msg: j.msg, size: j.size } : { state: "error", msg: "job not found" }));
    return;
  }
  if (req.method === "GET" && req.url.startsWith("/modfile")) {
    const id = new URL(req.url, "http://x").searchParams.get("job");
    const j = modJobs.get(id);
    if (!j || j.state !== "ready" || !fs.existsSync(j.file)) {
      res.writeHead(404);
      return res.end("not ready");
    }
    j.touched = Date.now();
    res.writeHead(200, { "Content-Type": "application/zip", "Content-Length": j.size });
    fs.createReadStream(j.file).pipe(res);
    return;
  }

  // player count for the online menu
  if (req.method === "GET" && req.url.startsWith("/online")) {
    let online = 0;
    for (const c of sessions.values()) if (!c.kickMsg) online++;
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    res.end(JSON.stringify({ online, searching: queue.length, matches: rooms.size }));
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
    const limit = c.hostCode && !c.kickMsg ? HOST_TIMEOUT_MS : SESSION_TIMEOUT_MS;
    if (now - c.lastSeen > limit) {
      c.hostCode = null; // dont delete the room, they might come back
      removeSession(c);
    }
  }
  for (const [code, r] of privateRooms) {
    const hostOn = findSessionByName(r.name);
    if (now - r.made > ROOM_LIFE_MS && !(hostOn && hostOn.hostCode === code)) privateRooms.delete(code);
  }
}, 5000);

loadAllAccounts();

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
