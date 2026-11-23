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
const PROTOCOL = 9;
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

// ------------------------------------------------------------ ranked stats + friends
const memoryStats = new Map();
const statsCache = new Map(); // lower name -> stats (saves database reads)
const memoryFriends = new Map(); // lower name -> Set of lower names

function newStats() {
  return { w: 0, l: 0, streak: 0, best: 0, rating: 1000, games: 0 };
}

async function getStats(name) {
  const key = String(name || "").toLowerCase();
  if (statsCache.has(key)) return statsCache.get(key);
  let st = null;
  try {
    if (!DB_URL || !DB_TOKEN) st = memoryStats.get(key) || null;
    else {
      const v = await db(["GET", "stats:" + key]);
      if (v) st = JSON.parse(v);
    }
  } catch (e) {
    console.log("stats read failed:", e.message);
  }
  st = Object.assign(newStats(), st || {});
  statsCache.set(key, st);
  return st;
}

async function saveStats(name, st) {
  const key = String(name || "").toLowerCase();
  statsCache.set(key, st);
  if (!DB_URL || !DB_TOKEN) {
    memoryStats.set(key, st);
    return;
  }
  try {
    await db(["SET", "stats:" + key, JSON.stringify(st)]);
    await db(["ZADD", "lb", String(Math.round(st.rating)), key]);
  } catch (e) {
    console.log("stats save failed:", e.message);
  }
}

function rankName(r) {
  if (r < 900) return "BRONZE";
  if (r < 1050) return "SILVER";
  if (r < 1200) return "GOLD";
  if (r < 1350) return "PLATINUM";
  if (r < 1500) return "DIAMOND";
  return "CHAMPION";
}

function displayName(key) {
  return allAccounts.get(key) || key;
}

function publicStats(name, st) {
  return {
    name: displayName(String(name).toLowerCase()) || name,
    rating: Math.round(st.rating),
    rank: rankName(st.rating),
    w: st.w,
    l: st.l,
    streak: st.streak,
    best: st.best,
    owner: isOwnerName(name),
  };
}

// winner gets points, loser loses them (elo, 32 max per match)
async function applyRanked(winName, loseName) {
  const sw = await getStats(winName);
  const sl = await getStats(loseName);
  const expected = 1 / (1 + Math.pow(10, (sl.rating - sw.rating) / 400));
  const delta = Math.max(1, Math.round(32 * (1 - expected)));
  sw.rating += delta;
  sl.rating = Math.max(0, sl.rating - delta);
  sw.w++;
  sw.games++;
  sw.streak = sw.streak > 0 ? sw.streak + 1 : 1;
  sw.best = Math.max(sw.best, sw.streak);
  sl.l++;
  sl.games++;
  sl.streak = 0;
  await saveStats(winName, sw);
  await saveStats(loseName, sl);
  console.log(`ranked: ${winName} beat ${loseName} (+/-${delta})`);
  return {
    win: { change: delta, rating: Math.round(sw.rating), rank: rankName(sw.rating) },
    lose: { change: -delta, rating: Math.round(sl.rating), rank: rankName(sl.rating) },
  };
}

async function leaderboard(me) {
  let keys = [];
  try {
    if (!DB_URL || !DB_TOKEN) {
      keys = [...memoryStats.entries()].filter(([, st]) => st.games > 0).sort((a, b) => b[1].rating - a[1].rating).slice(0, 10).map(([k]) => k);
    } else {
      const r = await db(["ZREVRANGE", "lb", "0", "9"]);
      keys = r || [];
    }
  } catch (e) {
    console.log("leaderboard failed:", e.message);
  }
  const top = [];
  for (const k of keys) top.push(publicStats(k, await getStats(k)));
  let mine = null;
  const meKey = String(me || "").toLowerCase();
  if (meKey && allAccounts.has(meKey)) {
    mine = publicStats(meKey, await getStats(meKey));
    try {
      if (DB_URL && DB_TOKEN) {
        const place = await db(["ZREVRANK", "lb", meKey]);
        mine.place = place === null || place === undefined ? null : Number(place) + 1;
      }
    } catch {}
  }
  return { top, me: mine };
}

async function getFriends(name) {
  const key = String(name).toLowerCase();
  if (!DB_URL || !DB_TOKEN) return [...(memoryFriends.get(key) || [])];
  try {
    return (await db(["SMEMBERS", "friends:" + key])) || [];
  } catch {
    return [];
  }
}

async function setFriend(name, other, add) {
  const key = String(name).toLowerCase();
  const o = String(other).toLowerCase();
  if (!DB_URL || !DB_TOKEN) {
    if (!memoryFriends.has(key)) memoryFriends.set(key, new Set());
    if (add) memoryFriends.get(key).add(o);
    else memoryFriends.get(key).delete(o);
    return;
  }
  await db([add ? "SADD" : "SREM", "friends:" + key, o]);
}

function statusOf(s) {
  if (!s) return "offline";
  if (s.room) return "in a match";
  if (s.spectating) return "spectating";
  if (queue.includes(s.sid)) return "searching";
  if (s.hostCode) return "private room";
  return "online";
}

async function sendFriends(c) {
  const list = [];
  for (const k of await getFriends(c.name)) {
    const s = findSessionByName(k);
    list.push({ name: displayName(k), status: statusOf(s), owner: isOwnerName(k) });
  }
  // online first, then by name
  list.sort((a, b) => (a.status === "offline") - (b.status === "offline") || a.name.toLowerCase().localeCompare(b.name.toLowerCase()));
  send(c, { t: "friends", list });
}

// announcements from the owner (shown to everyone for a while)
let lastAnnounce = null; // {msg, id, at}

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

    // 4) repack as a plain zip the game can read, inside one folder named after the mod
    //    (phones install it with a Shortcut that just unzips into the mods folder)
    job.state = "packing";
    const zip = path.join(work, "mod.zip");
    const folder = String(job.name || "mod").replace(/[^A-Za-z0-9 ._-]/g, "_").slice(0, 60) || "mod";
    const packDir = path.join(work, "pack");
    fs.mkdirSync(packDir, { recursive: true });
    fs.renameSync(root, path.join(packDir, folder));
    await run7z(["a", "-tzip", "-mx=1", "-bso0", "-bsp0", zip, folder], packDir);
    fs.rmSync(packDir, { recursive: true, force: true });
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

function startModJob(url, name) {
  const id = crypto.createHash("sha1").update(url + "|" + (name || "")).digest("hex").slice(0, 16);
  const existing = modJobs.get(id);
  if (existing && existing.state !== "error") {
    existing.touched = Date.now();
    return id;
  }
  modJobs.set(id, { state: "queued", pct: 0, msg: "", file: null, size: 0, touched: Date.now(), name: name || "mod" });
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
  const room = rooms.get(c.room);
  const opp = opponentOf(c);
  rooms.delete(c.room);
  c.room = null;
  if (room) {
    if (room.timer) clearTimeout(room.timer);
    for (const sid of room.spectators || []) {
      const sp = sessions.get(sid);
      if (sp) {
        sp.spectating = null;
        send(sp, { t: "spec_end", msg: c.name + " left the match" });
      }
    }
  }
  if (opp) {
    opp.room = null;
    send(opp, { t: "opponent_left" });
    // leaving in the middle of a ranked match = u lose it
    const midMatch = room && !room.over && (room.inSong || room.wins[room.a] > 0 || room.wins[room.b] > 0);
    if (room && room.ranked && midMatch) {
      applyRanked(opp.name, c.name)
        .then((r) => {
          send(opp, { t: "rank_update", won: true, ...r.win, forfeit: true });
          if (sessions.has(c.sid)) send(c, { t: "rank_update", won: false, ...r.lose, forfeit: true });
        })
        .catch((e) => console.log("ranked forfeit failed:", e.message));
    }
  }
}

// ---------- best of 3: both players report their score after each song
async function onResult(c, msg) {
  const room = rooms.get(c.room);
  if (!room) return;
  if (room.over) resetSeries(room);
  room.reports[c.sid] = { score: Math.max(0, Number(msg.score) || 0), misses: Math.max(0, Number(msg.misses) || 0) };
  if (room.reports[room.a] && room.reports[room.b]) decideRound(room);
  else if (!room.timer) room.timer = setTimeout(() => decideRound(room), 60000); // the other one never finished
}

// rematch after a finished match = brand new best of 3
function resetSeries(room) {
  room.over = false;
  room.wins[room.a] = 0;
  room.wins[room.b] = 0;
  room.round = 1;
  room.reports = {};
}

async function decideRound(room) {
  if (room.timer) clearTimeout(room.timer);
  room.timer = null;
  if (!rooms.has(room.id)) return;
  room.inSong = false;
  const ra = room.reports[room.a];
  const rb = room.reports[room.b];
  room.reports = {};
  const sa = ra ? ra.score : -1;
  const sb = rb ? rb.score : -1;
  const winner = sa > sb ? room.a : sb > sa ? room.b : null;
  if (winner) room.wins[winner]++;
  const roundNo = room.round++;
  let over = room.wins[room.a] >= 2 || room.wins[room.b] >= 2;
  if (!over && roundNo >= 5) over = true; // too many ties, call it
  let seriesWinner = null;
  if (over) {
    room.over = true;
    seriesWinner = room.wins[room.a] > room.wins[room.b] ? room.a : room.wins[room.b] > room.wins[room.a] ? room.b : null;
  }
  const A = sessions.get(room.a);
  const B = sessions.get(room.b);
  let ranked = null;
  if (over && seriesWinner && room.ranked && A && B) {
    try {
      const wName = seriesWinner === room.a ? A.name : B.name;
      const lName = seriesWinner === room.a ? B.name : A.name;
      ranked = await applyRanked(wName, lName);
    } catch (e) {
      console.log("ranked failed:", e.message);
    }
  }
  for (const [me, them] of [
    [room.a, room.b],
    [room.b, room.a],
  ]) {
    const p = sessions.get(me);
    if (!p) continue;
    const msg = {
      t: "round",
      round: roundNo,
      won: winner === null ? null : winner === me,
      me: room.wins[me],
      them: room.wins[them],
      over,
      seriesWon: over ? (seriesWinner === null ? null : seriesWinner === me) : null,
      ranked: room.ranked,
    };
    if (ranked) Object.assign(msg, seriesWinner === me ? ranked.win : ranked.lose);
    send(p, msg);
  }
  const wins = {};
  if (A) wins[A.name] = room.wins[room.a];
  if (B) wins[B.name] = room.wins[room.b];
  for (const sid of room.spectators) {
    send(sessions.get(sid), { t: "spec_round", round: roundNo, winner: winner ? sessions.get(winner)?.name || null : null, wins, over });
  }
}

function broadcastQueueCount() {
  for (const id of queue) send(sessions.get(id), { t: "queued", count: queue.length });
}

function makeMatch(a, b, isPrivate) {
  const roomId = "r" + newId();
  const song = SONGS[Math.floor(Math.random() * SONGS.length)];
  const room = {
    id: roomId,
    a: a.sid,
    b: b.sid,
    song,
    ranked: !isPrivate, // private rooms dont change ur rank
    wins: { [a.sid]: 0, [b.sid]: 0 },
    round: 1,
    reports: {},
    over: false,
    inSong: false,
    timer: null,
    spectators: new Set(),
  };
  rooms.set(roomId, room);
  a.room = b.room = roomId;
  a.spectating = b.spectating = null;
  console.log(`match${isPrivate ? " (private)" : ""}: ${a.name} vs ${b.name}`);
  // send it after looking up their profiles
  Promise.all([getStats(a.name), getStats(b.name)])
    .catch(() => [newStats(), newStats()])
    .then(([sa, sb]) => {
      const pa = publicStats(a.name, sa);
      const pb = publicStats(b.name, sb);
      // a plays as bf (picks the song), b as dad
      send(a, { t: "match", room: roomId, song, side: "bf", opponent: b.name, private: !!isPrivate, ranked: room.ranked, me: pa, opp: pb });
      send(b, { t: "match", room: roomId, song, side: "dad", opponent: a.name, private: !!isPrivate, ranked: room.ranked, me: pb, opp: pa });
    });
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
        .map((s) => ({ name: s.name, status: statusOf(s) === "online" ? "menu" : statusOf(s) }));
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
    case "relay": {
      // gameplay stuff (ready, score, done) goes straight to the opponent
      send(opponentOf(c), { t: "relay", data: msg.data });
      const room = rooms.get(c.room);
      if (room && msg.data && typeof msg.data === "object") {
        const ty = msg.data.type;
        if (ty === "ready") {
          if (room.over) resetSeries(room);
          room.inSong = true;
        }
        // spectators see the scores live
        if (room.spectators.size > 0 && (ty === "score" || ty === "done" || ty === "pick" || ty === "ready")) {
          for (const sid of room.spectators) send(sessions.get(sid), { t: "spec", from: c.name, data: msg.data });
        }
      }
      break;
    }
    case "result":
      await onResult(c, msg);
      break;
    // ---------- friends
    case "friends_list":
      await sendFriends(c);
      break;
    case "friend_add": {
      const key = String(msg.name || "").toLowerCase();
      if (!key || key === c.name.toLowerCase()) return send(c, { t: "friend_msg", msg: "cant add urself lol" });
      if (!allAccounts.has(key) && !(await getAccount(key))) return send(c, { t: "friend_msg", msg: "theres no account called " + msg.name });
      const list = await getFriends(c.name);
      if (list.length >= 100) return send(c, { t: "friend_msg", msg: "friends list is full (100)" });
      await setFriend(c.name, key, true);
      send(c, { t: "friend_msg", msg: "added " + displayName(key) + "!" });
      await sendFriends(c);
      break;
    }
    case "friend_remove": {
      await setFriend(c.name, String(msg.name || ""), false);
      send(c, { t: "friend_msg", msg: "removed " + msg.name });
      await sendFriends(c);
      break;
    }
    case "invite": {
      const code = String(msg.code || "").toUpperCase();
      const entry = privateRooms.get(code);
      if (!entry || entry.name.toLowerCase() !== c.name.toLowerCase()) return send(c, { t: "error", code: "room", msg: "make a room first" });
      const target = findSessionByName(String(msg.name || ""));
      if (!target) return send(c, { t: "error", code: "room", msg: msg.name + " isnt online rn" });
      if (target.room) return send(c, { t: "error", code: "room", msg: msg.name + " is in a match rn" });
      send(target, { t: "invited", from: c.name, code });
      send(c, { t: "invite_sent", to: target.name });
      break;
    }
    // ---------- spectating
    case "spectate": {
      const target = findSessionByName(String(msg.name || ""));
      const room = target ? rooms.get(target.room) : null;
      if (!room) return send(c, { t: "spec_end", msg: (msg.name || "they") + " isnt in a match rn" });
      leaveQueue(c);
      leaveRoom(c);
      closeHostedRoom(c);
      for (const r of rooms.values()) r.spectators.delete(c.sid);
      room.spectators.add(c.sid);
      c.spectating = room.id;
      const A = sessions.get(room.a);
      const B = sessions.get(room.b);
      send(c, { t: "spec_start", a: A ? A.name : "?", b: B ? B.name : "?", wins: [room.wins[room.a], room.wins[room.b]], round: room.round });
      break;
    }
    case "spectate_stop":
      for (const r of rooms.values()) r.spectators.delete(c.sid);
      c.spectating = null;
      break;
    // ---------- owner announcement
    case "admin_announce": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const text = String(msg.msg || "").replace(/[\r\n]+/g, " ").slice(0, 120).trim();
      if (!text) return;
      lastAnnounce = { msg: text, id: newId(), at: Date.now() };
      for (const s of sessions.values()) if (!s.kickMsg) send(s, { t: "announce", msg: text, id: lastAnnounce.id });
      console.log(`announcement from ${c.name}: ${text}`);
      send(c, { t: "admin_done", msg: "announced to everyone" });
      break;
    }
    case "bye":
      removeSession(c);
      break;
    default:
      break;
  }
}

function removeSession(c) {
  if (!sessions.has(c.sid)) return;
  for (const r of rooms.values()) r.spectators.delete(c.sid);
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
    if (lastAnnounce && Date.now() - lastAnnounce.at < 10 * 60000) send(c, { t: "announce", msg: lastAnnounce.msg, id: lastAnnounce.id });
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
      res.end(JSON.stringify({ ok: true, job: startModJob(body.url, typeof body.name === "string" ? body.name : "") }));
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
    res.writeHead(200, { "Content-Type": "application/zip", "Content-Length": j.size, "Content-Disposition": 'attachment; filename="mod.zip"' });
    fs.createReadStream(j.file).pipe(res);
    return;
  }

  // player count for the online menu
  if (req.method === "GET" && req.url.startsWith("/online")) {
    let online = 0;
    for (const c of sessions.values()) if (!c.kickMsg) online++;
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    const ann = lastAnnounce && Date.now() - lastAnnounce.at < 10 * 60000 ? lastAnnounce : null;
    res.end(JSON.stringify({ online, searching: queue.length, matches: rooms.size, announce: ann ? ann.msg : null, announceId: ann ? ann.id : null }));
    return;
  }

  // top 10 + ur own rank
  if (req.method === "GET" && req.url.startsWith("/leaderboard")) {
    const me = new URL(req.url, "http://x").searchParams.get("me") || "";
    leaderboard(me)
      .then((data) => {
        res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
        res.end(JSON.stringify(data));
      })
      .catch(() => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ top: [], me: null }));
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
