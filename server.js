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
const PROTOCOL = 18;
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

// removes an account + its rank, friends, requests, badge (used by delete + rename)
async function deleteAccount(key) {
  for (const f of await getFriends(key)) await setFriend(f, key, false);
  allAccounts.delete(key);
  statsCache.delete(key);
  delete badgeCache[key];
  if (!DB_URL || !DB_TOKEN) {
    memoryAccounts.delete(key);
    memoryStats.delete(key);
    memoryFriends.delete(key);
    return;
  }
  for (const k of ["acct:", "stats:", "friends:", "freq:", "unread:"]) await db(["DEL", k + key]).catch(() => {});
  await db(["ZREM", "lb", key]).catch(() => {});
  await db(["HDEL", "badges", key]).catch(() => {});
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
  const on = !DB_URL || !DB_TOKEN ? memoryBans.has(key) : (await db(["SISMEMBER", "bans", key])) === 1;
  if (!on) return false;
  // temp bans run out by themselves
  const info = await getKV("baninfo", key);
  if (info && info.until && Date.now() > info.until) {
    await setBanned(key, false);
    return false;
  }
  return true;
}

// small json values in a hash ("baninfo", "mutes", "badges")
const memoryKV = new Map();
async function getKV(hash, key) {
  if (!DB_URL || !DB_TOKEN) {
    const m = memoryKV.get(hash);
    return m && m.has(key) ? m.get(key) : null;
  }
  try {
    const v = await db(["HGET", hash, key]);
    return v ? JSON.parse(v) : null;
  } catch {
    return null;
  }
}
async function setKV(hash, key, value) {
  if (!DB_URL || !DB_TOKEN) {
    if (!memoryKV.has(hash)) memoryKV.set(hash, new Map());
    if (value === null) memoryKV.get(hash).delete(key);
    else memoryKV.get(hash).set(key, value);
    return;
  }
  if (value === null) await db(["HDEL", hash, key]);
  else await db(["HSET", hash, key, JSON.stringify(value)]);
}
async function getAllKV(hash) {
  const out = {};
  if (!DB_URL || !DB_TOKEN) {
    for (const [k, v] of memoryKV.get(hash) || []) out[k] = v;
    return out;
  }
  try {
    const r = (await db(["HGETALL", hash])) || [];
    for (let i = 0; i + 1 < r.length; i += 2) {
      try {
        out[r[i]] = JSON.parse(r[i + 1]);
      } catch {}
    }
  } catch {}
  return out;
}

function timeLeft(until) {
  if (!until) return "forever";
  const mins = Math.max(1, Math.round((until - Date.now()) / 60000));
  if (mins < 60) return mins + " min";
  if (mins < 2880) {
    const h = Math.round(mins / 60);
    return h + (h === 1 ? " hour" : " hours");
  }
  const d = Math.round(mins / 1440);
  return d + (d === 1 ? " day" : " days");
}

async function banMessage(key) {
  const info = await getKV("baninfo", key);
  let m = "this account is banned";
  if (info) {
    m += info.until ? " for " + timeLeft(info.until) : " forever";
    if (info.reason) m += ": " + info.reason;
  }
  return m;
}

// badges like VIP / TESTER (cached in memory, theres not many)
let badgeCache = {};
function badgeOf(name) {
  return badgeCache[String(name).toLowerCase()] || null;
}

// server settings the owner changes in game (maintenance, news popup, blocked mods)
let serverConfig = { maintenance: false, news: null, blockedMods: [] };
async function loadConfig() {
  try {
    if (DB_URL && DB_TOKEN) {
      const v = await db(["GET", "config"]);
      if (v) serverConfig = Object.assign(serverConfig, JSON.parse(v));
    }
    badgeCache = await getAllKV("badges");
  } catch (e) {
    console.log("config load failed:", e.message);
  }
}
async function saveConfig() {
  if (DB_URL && DB_TOKEN) await db(["SET", "config", JSON.stringify(serverConfig)]).catch(() => {});
}
function simpleKey(t) {
  return String(t || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

// reports from players
const memoryReports = [];
async function addReport(r) {
  if (!DB_URL || !DB_TOKEN) {
    memoryReports.push(r);
    while (memoryReports.length > 100) memoryReports.shift();
    return;
  }
  await db(["RPUSH", "reports", JSON.stringify(r)]);
  await db(["LTRIM", "reports", "-100", "-1"]);
}
async function getReports() {
  if (!DB_URL || !DB_TOKEN) return memoryReports.slice();
  try {
    return ((await db(["LRANGE", "reports", "0", "-1"])) || []).map((x) => {
      try {
        return JSON.parse(x);
      } catch {
        return null;
      }
    }).filter((x) => x);
  } catch {
    return [];
  }
}
async function setReports(list) {
  if (!DB_URL || !DB_TOKEN) {
    memoryReports.length = 0;
    memoryReports.push(...list);
    return;
  }
  await db(["DEL", "reports"]);
  for (const r of list) await db(["RPUSH", "reports", JSON.stringify(r)]);
}

// todays numbers for the owner stats page (reset every day, UTC)
let today = { day: new Date().toISOString().slice(0, 10), peak: 0, matches: 0, newAccounts: 0, songs: {} };
function todayStats() {
  const d = new Date().toISOString().slice(0, 10);
  if (today.day !== d) today = { day: d, peak: 0, matches: 0, newAccounts: 0, songs: {} };
  return today;
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
    badge: badgeOf(name),
    pfp: PFPS.includes(st.pfp) ? st.pfp : "bf",
  };
}

// profile pictures (character icons from the base game)
const PFPS = ["bf", "gf", "dad", "spooky", "pico", "mom", "parents-christmas", "monster", "senpai-pixel", "spirit-pixel", "tankman", "darnell", "nene", "bf-pixel"];
const NAME_COOLDOWN = 30 * 24 * 60 * 60 * 1000; // change ur name once a month

// moves an account to a new name (rank, friends, badge). returns an error text or null
async function renameAccount(oldKey, newName) {
  const newKey = newName.toLowerCase();
  if (!validName(newName)) return "bad name (3-16, no spaces)";
  if (newKey !== oldKey && (allAccounts.has(newKey) || (await getAccount(newKey)))) return newName + " is taken";
  const acct = await getAccount(oldKey);
  if (!acct) return "no account called " + oldKey;
  acct.name = newName;
  if (newKey === oldKey) {
    // just changing CAPS
    if (DB_URL && DB_TOKEN) await db(["SET", "acct:" + oldKey, JSON.stringify(acct)]);
    else memoryAccounts.set(oldKey, acct);
    allAccounts.set(oldKey, newName);
    return null;
  }
  if (!(await createAccount(newKey, acct))) return "couldnt rename, try again";
  const st = await getStats(oldKey);
  await saveStats(newKey, st);
  const friends = await getFriends(oldKey);
  for (const f of friends) {
    await setFriend(newKey, f, true);
    await setFriend(f, oldKey, false);
    await setFriend(f, newKey, true);
  }
  const badge = badgeOf(oldKey);
  if (badge) {
    await setKV("badges", newKey, badge);
    badgeCache[newKey] = badge;
  }
  await deleteAccount(oldKey);
  allAccounts.set(newKey, newName);
  return null;
}

// global chat (everyone online)
const globalChat = [];
let globalLoaded = false;
async function loadGlobalChat() {
  if (globalLoaded) return;
  globalLoaded = true;
  if (!DB_URL || !DB_TOKEN) return;
  try {
    const list = (await db(["LRANGE", "gchat", "0", "-1"])) || [];
    for (const x of list) {
      try {
        globalChat.push(JSON.parse(x));
      } catch {}
    }
  } catch {}
}
async function addGlobalChat(m) {
  globalChat.push(m);
  while (globalChat.length > 60) globalChat.shift();
  if (DB_URL && DB_TOKEN) {
    await db(["RPUSH", "gchat", JSON.stringify(m)]).catch(() => {});
    await db(["LTRIM", "gchat", "-60", "-1"]).catch(() => {});
  }
}

function tellOwners(text) {
  for (const s of sessions.values()) if (isOwnerName(s.name)) send(s, { t: "admin_done", msg: text });
}

// cheat check: too many 100% songs in a row = the owner gets told
async function checkCheat(c, acc) {
  const st = await getStats(c.name);
  const list = Array.isArray(st.accs) ? st.accs : [];
  list.push(Math.round((Number(acc) || 0) * 100) / 100);
  while (list.length > 10) list.shift();
  st.accs = list;
  await saveStats(c.name, st);
  const perfect = list.filter((a) => a >= 100).length;
  if (perfect >= 4 && perfect >= list.length * 0.6) {
    const key = c.name.toLowerCase();
    const last = cheatAlerted.get(key) || 0;
    if (Date.now() - last < 6 * 60 * 60 * 1000) return; // once every 6 hours per player
    cheatAlerted.set(key, Date.now());
    await addReport({ from: "CHEAT ALERT", name: c.name, reason: "got 100% in " + perfect + " of their last " + list.length + " songs", at: Date.now() });
    tellOwners("CHEAT ALERT: " + c.name + " got 100% in " + perfect + " of " + list.length + " songs");
    console.log("cheat alert: " + c.name);
  }
}
const cheatAlerted = new Map();

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

// ---------- friend requests: "freq:<name>" = who asked to be their friend
const memoryRequests = new Map();

async function getRequests(name) {
  const key = String(name).toLowerCase();
  if (!DB_URL || !DB_TOKEN) return [...(memoryRequests.get(key) || [])];
  try {
    return (await db(["SMEMBERS", "freq:" + key])) || [];
  } catch {
    return [];
  }
}

async function setRequest(to, from, add) {
  const t = String(to).toLowerCase();
  const f = String(from).toLowerCase();
  if (!DB_URL || !DB_TOKEN) {
    if (!memoryRequests.has(t)) memoryRequests.set(t, new Set());
    if (add) memoryRequests.get(t).add(f);
    else memoryRequests.get(t).delete(f);
    return;
  }
  await db([add ? "SADD" : "SREM", "freq:" + t, f]);
}

async function sendRequests(c) {
  const list = [];
  for (const k of await getRequests(c.name)) list.push({ name: displayName(k), status: statusOf(findSessionByName(k)), owner: isOwnerName(k) });
  list.sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()));
  send(c, { t: "friend_requests", list });
}

async function makeFriends(a, b) {
  await setFriend(a, b, true);
  await setFriend(b, a, true);
  await setRequest(a, b, false);
  await setRequest(b, a, false);
}

async function sendFriends(c) {
  const list = [];
  const unread = await getUnread(c.name);
  for (const k of await getFriends(c.name)) {
    const s = findSessionByName(k);
    list.push({ name: displayName(k), status: statusOf(s), owner: isOwnerName(k), badge: badgeOf(k), unread: unread[k] || 0 });
  }
  // online first, then by name
  list.sort((a, b) => (a.status === "offline") - (b.status === "offline") || a.name.toLowerCase().localeCompare(b.name.toLowerCase()));
  send(c, { t: "friends", list, requests: (await getRequests(c.name)).length });
}

// ---------- friend chat (last 50 messages per pair, unread counts)
const memoryChats = new Map(); // pairKey -> [msgs]
const memoryUnread = new Map(); // lower name -> Map(from -> count)

function pairKey(a, b) {
  const x = String(a).toLowerCase();
  const y = String(b).toLowerCase();
  return x < y ? x + "|" + y : y + "|" + x;
}

async function chatHistory(a, b) {
  const key = pairKey(a, b);
  if (!DB_URL || !DB_TOKEN) return memoryChats.get(key) || [];
  try {
    const list = (await db(["LRANGE", "chat:" + key, "-50", "-1"])) || [];
    return list.map((x) => {
      try {
        return JSON.parse(x);
      } catch {
        return null;
      }
    }).filter((x) => x);
  } catch {
    return [];
  }
}

async function chatStore(a, b, msg) {
  const key = pairKey(a, b);
  if (!DB_URL || !DB_TOKEN) {
    const list = memoryChats.get(key) || [];
    list.push(msg);
    while (list.length > 50) list.shift();
    memoryChats.set(key, list);
    return;
  }
  await db(["RPUSH", "chat:" + key, JSON.stringify(msg)]);
  await db(["LTRIM", "chat:" + key, "-50", "-1"]);
}

async function addUnread(to, from, n) {
  const t = String(to).toLowerCase();
  const f = String(from).toLowerCase();
  if (!DB_URL || !DB_TOKEN) {
    if (!memoryUnread.has(t)) memoryUnread.set(t, new Map());
    const m = memoryUnread.get(t);
    if (n === 0) m.delete(f);
    else m.set(f, (m.get(f) || 0) + n);
    return;
  }
  try {
    if (n === 0) await db(["HDEL", "unread:" + t, f]);
    else await db(["HINCRBY", "unread:" + t, f, String(n)]);
  } catch {}
}

async function getUnread(name) {
  const t = String(name).toLowerCase();
  const out = {};
  if (!DB_URL || !DB_TOKEN) {
    for (const [k, v] of memoryUnread.get(t) || []) out[k] = v;
    return out;
  }
  try {
    const r = (await db(["HGETALL", "unread:" + t])) || [];
    for (let i = 0; i + 1 < r.length; i += 2) out[r[i]] = Number(r[i + 1]) || 0;
  } catch {}
  return out;
}

const STICKER_RE = /^[A-Za-z0-9_\/.-]{1,140}$/;

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
    todayStats().newAccounts++;
    console.log(`new account: ${name}`);
    return { ok: true, created: true, name, token };
  }

  if (tooMany("acct:" + key, 8, 5 * 60000)) return { ok: false, error: "too many wrong passwords, wait 5 minutes" };
  if (await isBanned(key)) return { ok: false, error: await banMessage(key) };
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
  if (!room || room.party) return null;
  return sessions.get(room.a === c.sid ? room.b : room.a) || null;
}

function leaveQueue(c) {
  queue = queue.filter((id) => id !== c.sid);
  partyQueue = partyQueue.filter((id) => id !== c.sid);
}

// ---------- 3-4 player parties
// 4 people in the party queue = match. 3 people who waited 25s = match too
let partyQueue = [];
const PARTY_WAIT_3 = 25000;

function tryParty() {
  partyQueue = partyQueue.filter((id) => sessions.has(id) && !sessions.get(id).room);
  let made = true;
  while (made) {
    made = false;
    if (partyQueue.length >= 4) {
      makeParty(partyQueue.splice(0, 4).map((id) => sessions.get(id)));
      made = true;
    } else if (partyQueue.length === 3) {
      const oldest = Math.min(...partyQueue.map((id) => sessions.get(id).partyAt || Date.now()));
      if (Date.now() - oldest >= PARTY_WAIT_3) {
        makeParty(partyQueue.splice(0, 3).map((id) => sessions.get(id)));
        made = true;
      }
    }
  }
  for (const id of partyQueue) send(sessions.get(id), { t: "queued", count: partyQueue.length, party: true });
}
setInterval(tryParty, 2000);

function partyActive(room) {
  return room.slots.filter((sid) => !room.gone.has(sid) && sessions.has(sid));
}

function makeParty(players) {
  const roomId = "p" + newId();
  const song = SONGS[Math.floor(Math.random() * SONGS.length)];
  const slots = players.map((p) => p.sid);
  const room = {
    id: roomId,
    party: true,
    slots,
    names: players.map((p) => p.name),
    gone: new Set(),
    a: slots[0],
    b: slots[1],
    song,
    ranked: false,
    bestOf: 1,
    wins: {},
    round: 1,
    reports: {},
    over: false,
    inSong: false,
    timer: null,
    scores: {},
    spectators: new Set(),
  };
  for (const sid of slots) room.wins[sid] = 0;
  rooms.set(roomId, room);
  for (const p of players) {
    p.room = roomId;
    p.spectating = null;
  }
  console.log(`party: ${room.names.join(", ")}`);
  todayStats().matches++;
  Promise.all(players.map((p) => getStats(p.name)))
    .catch(() => players.map(() => newStats()))
    .then((sts) => {
      const profiles = players.map((p, i) => publicStats(p.name, sts[i]));
      players.forEach((p, i) => {
        send(p, {
          t: "match",
          party: true,
          room: roomId,
          song,
          slot: i,
          side: i === 0 ? "bf" : "dad",
          opponent: i === 0 ? "the others" : room.names[0],
          players: room.names,
          profiles,
          me: profiles[i],
          opp: null,
          private: false,
          ranked: false,
          bestOf: 1,
          common: [],
          modsOnly: false,
          rules: {},
        });
      });
    });
}

function partyLeave(c, room) {
  room.gone.add(c.sid);
  c.room = null;
  const slot = room.slots.indexOf(c.sid);
  const left = partyActive(room);
  if (left.length <= 1) {
    rooms.delete(room.id);
    if (room.timer) clearTimeout(room.timer);
    for (const sid of left) {
      const p = sessions.get(sid);
      p.room = null;
      send(p, { t: "opponent_left" });
    }
    for (const sid of room.spectators) {
      const sp = sessions.get(sid);
      if (sp) {
        sp.spectating = null;
        send(sp, { t: "spec_end", msg: "the party ended" });
      }
    }
    return;
  }
  for (const sid of left) send(sessions.get(sid), { t: "party_left", slot, name: c.name });
  // everyone else already finished the song: decide it now
  if (left.every((sid) => room.reports[sid])) decidePartyRound(room);
}

async function decidePartyRound(room) {
  if (room.timer) clearTimeout(room.timer);
  room.timer = null;
  if (!rooms.has(room.id)) return;
  room.inSong = false;
  let best = -1;
  let winner = null;
  let tie = false;
  for (const sid of room.slots) {
    const r = room.reports[sid];
    if (!r) continue;
    if (r.score > best) {
      best = r.score;
      winner = sid;
      tie = false;
    } else if (r.score === best) tie = true;
  }
  if (tie) winner = null;
  if (winner) room.wins[winner]++;
  const roundNo = room.round++;
  const need = room.bestOf === 1 ? 1 : 2;
  let over = room.slots.some((sid) => room.wins[sid] >= need) || roundNo >= (room.bestOf === 1 ? 1 : 5);
  let seriesWinner = null;
  if (over) {
    room.over = true;
    let bw = -1;
    let btie = false;
    for (const sid of room.slots) {
      if (room.wins[sid] > bw) {
        bw = room.wins[sid];
        seriesWinner = sid;
        btie = false;
      } else if (room.wins[sid] === bw) btie = true;
    }
    if (btie) seriesWinner = null;
  }
  const standings = room.slots.map((sid, i) => ({
    name: room.names[i],
    score: room.reports[sid] ? room.reports[sid].score : null,
    misses: room.reports[sid] ? room.reports[sid].misses : null,
    wins: room.wins[sid],
    left: room.gone.has(sid),
  }));
  room.reports = {};
  for (const sid of partyActive(room)) {
    const otherWins = Math.max(0, ...room.slots.filter((x) => x !== sid).map((x) => room.wins[x]));
    send(sessions.get(sid), {
      t: "round",
      party: true,
      round: roundNo,
      won: winner === null ? null : winner === sid,
      me: room.wins[sid],
      them: otherWins,
      over,
      seriesWon: over ? (seriesWinner === null ? null : seriesWinner === sid) : null,
      standings,
      ranked: false,
      bestOf: room.bestOf,
    });
  }
  const wins = {};
  room.slots.forEach((sid, i) => (wins[room.names[i]] = room.wins[sid]));
  for (const sid of room.spectators) {
    send(sessions.get(sid), { t: "spec_round", round: roundNo, winner: winner ? room.names[room.slots.indexOf(winner)] : null, wins, over });
  }
}

function leaveRoom(c) {
  if (!c.room) return;
  const room = rooms.get(c.room);
  if (room && room.party) return partyLeave(c, room);
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
  if (room.party) {
    if (partyActive(room).every((sid) => room.reports[sid])) decidePartyRound(room);
    else if (!room.timer) room.timer = setTimeout(() => decidePartyRound(room), 60000);
    return;
  }
  if (room.reports[room.a] && room.reports[room.b]) decideRound(room);
  else if (!room.timer) room.timer = setTimeout(() => decideRound(room), 60000); // the other one never finished
}

// rematch after a finished match = brand new best of 3
function resetSeries(room) {
  room.over = false;
  if (room.party) {
    for (const sid of room.slots) room.wins[sid] = 0;
    room.round = 1;
    room.reports = {};
    return;
  }
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
  const need = room.bestOf === 1 ? 1 : 2;
  let over = room.wins[room.a] >= need || room.wins[room.b] >= need;
  if (!over && roundNo >= (room.bestOf === 1 ? 1 : 5)) over = true; // normal round ends after 1 song, best of 3 calls it after 5 ties
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
      bestOf: room.bestOf,
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

function makeMatch(a, b, isPrivate, opts) {
  opts = opts || {};
  const bestOf = opts.bestOf === 1 ? 1 : 3;
  const roomId = "r" + newId();
  const song = SONGS[Math.floor(Math.random() * SONGS.length)];
  const room = {
    id: roomId,
    a: a.sid,
    b: b.sid,
    song,
    ranked: !isPrivate, // private rooms dont change ur rank
    bestOf,
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
  todayStats().matches++;
  // send it after looking up their profiles
  Promise.all([getStats(a.name), getStats(b.name)])
    .catch(() => [newStats(), newStats()])
    .then(([sa, sb]) => {
      const pa = publicStats(a.name, sa);
      const pb = publicStats(b.name, sb);
      // a plays as bf (picks the song), b as dad
      const extra = { bestOf, common: opts.common || [], modsOnly: !!opts.modsOnly, rules: opts.rules || {} };
      send(a, { t: "match", room: roomId, song, side: "bf", opponent: b.name, private: !!isPrivate, ranked: room.ranked, me: pa, opp: pb, ...extra });
      send(b, { t: "match", room: roomId, song, side: "dad", opponent: a.name, private: !!isPrivate, ranked: room.ranked, me: pb, opp: pa, ...extra });
    });
}

// private room modifiers
function cleanRules(r) {
  r = r && typeof r === "object" ? r : {};
  return { sudden: r.sudden === true, fast: r.fast === true, sick: r.sick === true };
}

function commonMods(a, b) {
  const set = new Set(a.mods || []);
  return (b.mods || []).filter((m) => set.has(m));
}

// pairs people up: same mode (normal / best of 3), and if either wants matching mods they must share one
// (the most shared mods wins when theres a choice)
function tryMatch() {
  for (const sid of [...queue]) if (!sessions.has(sid)) queue.splice(queue.indexOf(sid), 1);
  let found = true;
  while (found) {
    found = false;
    for (let i = 0; i < queue.length && !found; i++) {
      const a = sessions.get(queue[i]);
      let best = null;
      let bestCommon = null;
      for (let j = i + 1; j < queue.length; j++) {
        const b = sessions.get(queue[j]);
        if (!b || (a.queueMode || "bo3") !== (b.queueMode || "bo3")) continue;
        const common = commonMods(a, b);
        if ((a.matchMods || b.matchMods) && common.length === 0) continue;
        if (!best || common.length > bestCommon.length) {
          best = b;
          bestCommon = common;
        }
        if (!a.matchMods && !b.matchMods) break; // first fit is fine
      }
      if (best) {
        queue.splice(queue.indexOf(best.sid), 1);
        queue.splice(queue.indexOf(a.sid), 1);
        makeMatch(a, best, false, { bestOf: a.queueMode === "normal" ? 1 : 3, common: bestCommon, modsOnly: !!(a.matchMods || best.matchMods) });
        found = true;
      }
    }
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
      if (serverConfig.maintenance && !isOwnerName(c.name)) return send(c, { t: "error", code: "maint", msg: "Server under maintenance, be back soon!" });
      leaveRoom(c);
      closeHostedRoom(c);
      if (msg.party === true) {
        leaveQueue(c);
        c.partyAt = Date.now();
        partyQueue.push(c.sid);
        tryParty();
        break;
      }
      c.queueMode = msg.mode === "normal" ? "normal" : "bo3";
      c.matchMods = msg.matchMods === true;
      c.mods = Array.isArray(msg.mods) ? msg.mods.slice(0, 300).map((m) => String(m).toLowerCase().slice(0, 60)).filter((m) => m) : [];
      if (!queue.includes(c.sid)) queue.push(c.sid);
      tryMatch();
      break;
    case "cancel":
      leaveQueue(c);
      closeHostedRoom(c);
      broadcastQueueCount();
      break;
    case "create_room": {
      if (serverConfig.maintenance && !isOwnerName(c.name)) return send(c, { t: "error", code: "room", msg: "Server under maintenance, be back soon!" });
      leaveQueue(c);
      leaveRoom(c);
      // reconnecting host asks for the same code back
      const want = String(msg.code || "").toUpperCase().trim();
      const old = want ? privateRooms.get(want) : null;
      if (old && old.name.toLowerCase() === c.name.toLowerCase()) {
        if (c.hostCode && c.hostCode !== want) closeHostedRoom(c);
        old.made = Date.now();
        if (msg.rules) old.rules = cleanRules(msg.rules);
        c.hostCode = want;
        return send(c, { t: "room_created", code: want });
      }
      closeHostedRoom(c);
      const code = newRoomCode();
      if (!code) return send(c, { t: "error", code: "room", msg: "couldnt make a room, try again" });
      privateRooms.set(code, { name: c.name, made: Date.now(), rules: cleanRules(msg.rules) });
      c.hostCode = code;
      console.log(`room ${code} made by ${c.name}`);
      send(c, { t: "room_created", code });
      break;
    }
    case "join_room": {
      if (serverConfig.maintenance && !isOwnerName(c.name)) return send(c, { t: "error", code: "room", msg: "Server under maintenance, be back soon!" });
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
      makeMatch(host, c, true, { rules: entry.rules });
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
        await setKV("baninfo", name.toLowerCase(), null);
        console.log(`owner ${c.name} unbanned ${name}`);
        return send(c, { t: "admin_done", msg: name + " unbanned" });
      }
      let banText = "u got banned by the owner";
      if (msg.t === "admin_ban") {
        await setBanned(name, true);
        const mins = Math.max(0, Number(msg.mins) || 0);
        const reason = String(msg.reason || "").slice(0, 80);
        await setKV("baninfo", name.toLowerCase(), { until: mins ? Date.now() + mins * 60000 : 0, reason });
        banText = "u got banned " + (mins ? "for " + timeLeft(Date.now() + mins * 60000) : "forever") + (reason ? ": " + reason : "");
      }
      for (const s of sessions.values()) {
        if (s.name.toLowerCase() === name.toLowerCase()) kickSession(s, msg.t === "admin_ban" ? banText : "u got kicked by the owner");
      }
      console.log(`owner ${c.name} ${msg.t === "admin_ban" ? "banned" : "kicked"} ${name}`);
      send(c, { t: "admin_done", msg: name + (msg.t === "admin_ban" ? " banned" : " kicked") });
      break;
    }
    // ---------- players change their own password (needs the old one)
    case "change_pass": {
      c.passTries = (c.passTries || 0) + 1;
      if (c.passTries > 5) return send(c, { t: "pass_result", ok: false, msg: "too many tries, wait a bit and log in again" });
      const key = c.name.toLowerCase();
      const acct = await getAccount(key);
      if (!acct) return send(c, { t: "pass_result", ok: false, msg: "account not found" });
      if (!sameHex(hashPassword(String(msg.old || ""), acct.salt), acct.hash)) return send(c, { t: "pass_result", ok: false, msg: "ur old password is wrong" });
      if (!validPassword(msg.pass)) return send(c, { t: "pass_result", ok: false, msg: "new password needs 6+ characters, no spaces" });
      acct.salt = crypto.randomBytes(16).toString("hex");
      acct.hash = hashPassword(String(msg.pass), acct.salt);
      // this device stays logged in, every other device has to log in again
      acct.tokens = c.tokenHash ? [c.tokenHash] : [];
      acct.tokenHash = acct.tokens[0] || null;
      if (!DB_URL || !DB_TOKEN) memoryAccounts.set(key, acct);
      else await db(["SET", "acct:" + key, JSON.stringify(acct)]);
      c.passTries = 0;
      console.log(`${c.name} changed their password`);
      send(c, { t: "pass_result", ok: true, msg: "password changed!" });
      break;
    }
    // ---------- reports (any player)
    case "report": {
      const key = String(msg.name || "").toLowerCase();
      if (!allAccounts.has(key) || key === c.name.toLowerCase()) return send(c, { t: "friend_msg", msg: "cant report that" });
      if (Date.now() - (c.lastReport || 0) < 30000) return send(c, { t: "friend_msg", msg: "wait a bit before reporting again" });
      c.lastReport = Date.now();
      await addReport({ from: c.name, name: displayName(key), reason: String(msg.reason || "").replace(/[\r\n]+/g, " ").slice(0, 120), at: Date.now() });
      send(c, { t: "friend_msg", msg: "reported " + displayName(key) + ", thanks!" });
      for (const s of sessions.values()) if (isOwnerName(s.name)) send(s, { t: "admin_done", msg: "new report about " + displayName(key) });
      break;
    }
    // ---------- owner: server pages
    case "admin_matches": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const list = [];
      for (const r of rooms.values()) {
        if (r.party) {
          const act = partyActive(r);
          list.push({
            a: act.map((sid) => sessions.get(sid).name).join(", "),
            b: "(" + act.length + " player party)",
            song: r.songName || "picking...",
            sa: Math.max(0, ...act.map((sid) => (r.scores && r.scores[sid]) || 0)),
            sb: 0,
            wa: 0,
            wb: 0,
            round: r.round,
            ranked: false,
            bestOf: r.bestOf,
            watchers: r.spectators.size,
          });
          continue;
        }
        const A = sessions.get(r.a);
        const B = sessions.get(r.b);
        if (!A || !B) continue;
        list.push({
          a: A.name,
          b: B.name,
          song: r.songName || "picking...",
          sa: (r.scores && r.scores[r.a]) || 0,
          sb: (r.scores && r.scores[r.b]) || 0,
          wa: r.wins[r.a],
          wb: r.wins[r.b],
          round: r.round,
          ranked: r.ranked,
          bestOf: r.bestOf,
          watchers: r.spectators.size,
        });
      }
      send(c, { t: "admin_matches", list });
      break;
    }
    case "admin_stats": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const td = todayStats();
      let online = 0;
      for (const s of sessions.values()) if (!s.kickMsg) online++;
      const top = Object.entries(td.songs).sort((x, y) => y[1] - x[1]).slice(0, 5);
      send(c, {
        t: "admin_stats",
        stats: { online, searching: queue.length, matches: rooms.size, rooms: privateRooms.size, peak: td.peak, matchesToday: td.matches, newAccounts: td.newAccounts, accounts: allAccounts.size, banned: (await listBans()).length, topSongs: top, upMins: Math.round(process.uptime() / 60) },
      });
      break;
    }
    case "admin_reports": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      send(c, { t: "admin_reports", list: await getReports() });
      break;
    }
    case "admin_clear_report": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      let list = await getReports();
      if (msg.all === true) list = [];
      else list = list.filter((r) => !(r.at === msg.at && r.from === msg.from));
      await setReports(list);
      send(c, { t: "admin_reports", list });
      break;
    }
    case "admin_settings": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      if (msg.maintenance === true || msg.maintenance === false) {
        serverConfig.maintenance = msg.maintenance;
        console.log(`maintenance ${msg.maintenance ? "ON" : "OFF"}`);
        if (msg.maintenance) {
          for (const s of sessions.values()) if (!isOwnerName(s.name)) send(s, { t: "announce", msg: "Server under maintenance, be back soon!", id: newId() });
          for (const sid of [...queue]) {
            const s = sessions.get(sid);
            if (s && !isOwnerName(s.name)) {
              leaveQueue(s);
              send(s, { t: "error", code: "maint", msg: "Server under maintenance, be back soon!" });
            }
          }
        }
      }
      if (typeof msg.news === "string") {
        const text = msg.news.slice(0, 300).trim();
        serverConfig.news = text ? { id: newId(), text } : null;
      }
      if (typeof msg.blockMod === "string") {
        const k = simpleKey(msg.blockMod);
        if (k && !serverConfig.blockedMods.includes(k)) serverConfig.blockedMods.push(k);
      }
      if (typeof msg.unblockMod === "string") serverConfig.blockedMods = serverConfig.blockedMods.filter((m) => m !== msg.unblockMod);
      await saveConfig();
      send(c, { t: "admin_settings", settings: serverConfig });
      break;
    }
    // ---------- owner: account tools
    case "admin_mute": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const key = String(msg.name || "").toLowerCase();
      const mins = Math.max(0, Number(msg.mins) || 0);
      if (msg.off === true) await setKV("mutes", key, null);
      else await setKV("mutes", key, { until: mins ? Date.now() + mins * 60000 : 0 });
      send(c, { t: "admin_done", msg: msg.off === true ? "unmuted " + displayName(key) : "muted " + displayName(key) + (mins ? " for " + timeLeft(Date.now() + mins * 60000) : " forever") });
      break;
    }
    case "admin_badge": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const key = String(msg.name || "").toLowerCase();
      const badge = String(msg.badge || "").toUpperCase().replace(/[^A-Z0-9 !?+-]/g, "").slice(0, 10).trim();
      await setKV("badges", key, badge ? badge : null);
      if (badge) badgeCache[key] = badge;
      else delete badgeCache[key];
      send(c, { t: "admin_done", msg: badge ? "gave " + displayName(key) + " the " + badge + " badge" : "removed the badge of " + displayName(key) });
      break;
    }
    case "admin_setrating": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const key = String(msg.name || "").toLowerCase();
      const r = Math.max(0, Math.min(5000, Math.round(Number(msg.rating) || 0)));
      const st = await getStats(key);
      st.rating = r;
      await saveStats(key, st);
      send(c, { t: "admin_done", msg: displayName(key) + " is now " + rankName(r) + " " + r });
      break;
    }
    case "admin_rename": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const oldKey = String(msg.name || "").toLowerCase();
      const newName = String(msg.newName || "");
      const err = await renameAccount(oldKey, newName);
      if (err) return send(c, { t: "admin_done", msg: err });
      for (const s of sessions.values()) if (s.name.toLowerCase() === oldKey) kickSession(s, "ur account got renamed to " + newName + ", log in with that name");
      console.log(`owner renamed ${oldKey} to ${newName}`);
      send(c, { t: "admin_done", msg: "renamed to " + newName });
      break;
    }
    case "admin_delete": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const key = String(msg.name || "").toLowerCase();
      if (isOwnerName(key)) return send(c, { t: "admin_done", msg: "cant delete an owner" });
      if (!allAccounts.has(key) && !(await getAccount(key))) return send(c, { t: "admin_done", msg: "no account called " + msg.name });
      for (const s of sessions.values()) if (s.name.toLowerCase() === key) kickSession(s, "ur account was deleted");
      const name = displayName(key);
      await deleteAccount(key);
      console.log(`owner deleted ${name}`);
      send(c, { t: "admin_done", msg: "deleted " + name });
      break;
    }
    // ---------- more owner tools
    case "admin_info": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const key = String(msg.name || "").toLowerCase();
      const acct = await getAccount(key);
      if (!acct) return send(c, { t: "admin_done", msg: "no account called " + msg.name });
      const s = findSessionByName(key);
      const room = s ? rooms.get(s.room) : null;
      let opponent = null;
      if (room && s) {
        const o = sessions.get(room.a === s.sid ? room.b : room.a);
        opponent = o ? o.name : null;
      }
      const st = await getStats(key);
      const friends = (await getFriends(key)).length;
      let hostedRoom = s && s.hostCode ? s.hostCode : null;
      if (!hostedRoom) for (const [code, r] of privateRooms) if (r.name.toLowerCase() === key) hostedRoom = code;
      send(c, {
        t: "admin_info",
        info: {
          name: acct.name,
          online: !!s,
          status: statusOf(s),
          roomCode: hostedRoom,
          opponent,
          match: room ? (room.ranked ? "ranked" : "private") + (room.bestOf === 1 ? ", normal round" : ", best of 3") + ", round " + room.round : null,
          spectating: s && s.spectating ? true : false,
          device: s && s.device ? s.device : null,
          onlineFor: s ? Math.round((Date.now() - s.joined) / 60000) : null,
          lastSeen: s ? null : await getSeen(key),
          created: acct.created || null,
          devices: (acct.tokens || [acct.tokenHash]).filter((x) => x).length,
          banned: await isBanned(key),
          owner: isOwnerName(key),
          friends,
          stats: publicStats(key, st),
          games: st.games,
        },
      });
      break;
    }
    case "admin_resetpass": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const key = String(msg.name || "").toLowerCase();
      if (isOwnerName(key)) return send(c, { t: "admin_done", msg: "cant do that to an owner" });
      const acct = await getAccount(key);
      if (!acct) return send(c, { t: "admin_done", msg: "no account called " + msg.name });
      // new random password, every logged in device gets logged out
      const temp = crypto.randomBytes(6).toString("base64").replace(/[^A-Za-z0-9]/g, "").slice(0, 8) || "fnf" + Math.floor(Math.random() * 99999);
      acct.salt = crypto.randomBytes(16).toString("hex");
      acct.hash = hashPassword(temp, acct.salt);
      acct.tokens = [];
      acct.tokenHash = null;
      if (!DB_URL || !DB_TOKEN) memoryAccounts.set(key, acct);
      else await db(["SET", "acct:" + key, JSON.stringify(acct)]);
      for (const s of sessions.values()) if (s.name.toLowerCase() === key) kickSession(s, "ur password was reset by the owner, log in again");
      console.log(`owner ${c.name} reset the password of ${acct.name}`);
      send(c, { t: "admin_done", msg: "new password for " + acct.name + ": " + temp, keep: true });
      break;
    }
    case "admin_dm": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const target = findSessionByName(String(msg.name || ""));
      const text = String(msg.msg || "").replace(/[\r\n]+/g, " ").slice(0, 120).trim();
      if (!target) return send(c, { t: "admin_done", msg: msg.name + " isnt online" });
      if (!text) return;
      send(target, { t: "announce", msg: "(message from the owner) " + text, id: newId() });
      send(c, { t: "admin_done", msg: "sent to " + target.name });
      break;
    }
    case "admin_resetrank": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const key = String(msg.name || "").toLowerCase();
      if (!allAccounts.has(key) && !(await getAccount(key))) return send(c, { t: "admin_done", msg: "no account called " + msg.name });
      const fresh = newStats();
      statsCache.set(key, fresh);
      if (!DB_URL || !DB_TOKEN) memoryStats.set(key, fresh);
      else {
        await db(["SET", "stats:" + key, JSON.stringify(fresh)]);
        await db(["ZREM", "lb", key]);
      }
      send(c, { t: "admin_done", msg: "reset the rank of " + displayName(key) });
      break;
    }
    case "admin_endmatch": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      const target = findSessionByName(String(msg.name || ""));
      const room = target ? rooms.get(target.room) : null;
      if (!room) return send(c, { t: "admin_done", msg: (msg.name || "they") + " isnt in a match" });
      room.over = true; // no rank change for this
      const who = room.party ? room.slots.map((sid) => sessions.get(sid)) : [sessions.get(room.a), sessions.get(room.b)];
      for (const p of who) if (p) kickSession(p, "the owner ended ur match");
      send(c, { t: "admin_done", msg: "ended the match of " + target.name });
      break;
    }
    case "leave":
      leaveRoom(c);
      break;
    case "relay": {
      const room = rooms.get(c.room);
      // blocked mods cant be picked online
      if (msg.data && msg.data.type === "pick" && msg.data.mod && serverConfig.blockedMods.length) {
        const k = simpleKey(msg.data.mod.title || msg.data.mod.id);
        if (serverConfig.blockedMods.includes(k)) return send(c, { t: "pick_blocked", name: msg.data.mod.title || msg.data.mod.id });
      }
      if (room && msg.data && typeof msg.data === "object") {
        if (msg.data.type === "pick") {
          room.songName = String(msg.data.name || msg.data.song || "?").slice(0, 60);
          room.scores = {};
          const td = todayStats();
          td.songs[room.songName] = (td.songs[room.songName] || 0) + 1;
        }
        if (msg.data.type === "score" || msg.data.type === "done") {
          if (!room.scores) room.scores = {};
          room.scores[c.sid] = Number(msg.data.score) || 0;
        }
      }
      // gameplay stuff (ready, score, done) goes straight to the opponent (party: to everyone else)
      if (room && room.party) {
        const slot = room.slots.indexOf(c.sid);
        for (const sid of partyActive(room)) if (sid !== c.sid) send(sessions.get(sid), { t: "relay", data: msg.data, slot, from: c.name });
      } else send(opponentOf(c), { t: "relay", data: msg.data });
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
      if (msg.acc != null) checkCheat(c, msg.acc).catch((e) => console.log("cheat check failed:", e.message));
      break;
    case "cheat_flag": {
      // the mod found a botplay file/mod on their pc
      if (c.flagged) break;
      c.flagged = true;
      const what = String(msg.what || "").replace(/[\r\n]+/g, " ").slice(0, 120);
      await addReport({ from: "CHEAT ALERT", name: c.name, reason: "botplay found: " + what, at: Date.now() });
      tellOwners("CHEAT ALERT: " + c.name + " has botplay (" + what + ")");
      break;
    }
    // ---------- profile
    case "profile_get": {
      const st = await getStats(c.name);
      const acct = await getAccount(c.name.toLowerCase());
      const left = acct && acct.renamedAt && !isOwnerName(c.name) ? Math.max(0, acct.renamedAt + NAME_COOLDOWN - Date.now()) : 0;
      send(c, { t: "profile", p: publicStats(c.name, st), noFR: !!st.noFR, nameLeft: left, pfps: PFPS });
      break;
    }
    case "profile_set": {
      const st = await getStats(c.name);
      if (typeof msg.pfp === "string" && PFPS.includes(msg.pfp)) st.pfp = msg.pfp;
      if (typeof msg.noFR === "boolean") st.noFR = msg.noFR;
      await saveStats(c.name, st);
      send(c, { t: "profile", p: publicStats(c.name, st), noFR: !!st.noFR, nameLeft: -1, pfps: PFPS });
      break;
    }
    case "change_name": {
      const oldKey = c.name.toLowerCase();
      const newName = String(msg.newName || "").trim();
      const acct = await getAccount(oldKey);
      if (!acct) return send(c, { t: "name_result", ok: false, msg: "log in again" });
      if (acct.renamedAt && !isOwnerName(c.name) && Date.now() < acct.renamedAt + NAME_COOLDOWN)
        return send(c, { t: "name_result", ok: false, msg: "u can change ur name again in " + timeLeft(acct.renamedAt + NAME_COOLDOWN) });
      if (c.room) return send(c, { t: "name_result", ok: false, msg: "finish ur match first" });
      const err = await renameAccount(oldKey, newName);
      if (err) return send(c, { t: "name_result", ok: false, msg: err });
      const fresh = await getAccount(newName.toLowerCase());
      if (fresh) {
        fresh.renamedAt = Date.now();
        if (DB_URL && DB_TOKEN) await db(["SET", "acct:" + newName.toLowerCase(), JSON.stringify(fresh)]);
        else memoryAccounts.set(newName.toLowerCase(), fresh);
      }
      console.log(`${oldKey} changed their name to ${newName}`);
      send(c, { t: "name_result", ok: true, name: newName, msg: "ur name is " + newName + " now!" });
      // the game reconnects with the new name (same login token)
      closeHostedRoom(c);
      leaveQueue(c);
      c.endAfter = true;
      break;
    }
    // ---------- global chat
    case "gchat_join":
      await loadGlobalChat();
      c.inGlobal = true;
      send(c, { t: "gchat_history", msgs: globalChat.slice(-40) });
      break;
    case "gchat_leave":
      c.inGlobal = false;
      break;
    case "gchat_send": {
      const text = String(msg.text || "").replace(/[\r\n]+/g, " ").slice(0, 150).trim();
      const sticker = typeof msg.sticker === "string" && STICKER_RE.test(msg.sticker) && !msg.sticker.includes("..") ? msg.sticker : null;
      if (!text && !sticker) return;
      const mute = await getKV("mutes", c.name.toLowerCase());
      if (mute && (!mute.until || Date.now() < mute.until)) return send(c, { t: "friend_msg", msg: "ur muted" + (mute.until ? " for " + timeLeft(mute.until) : "") });
      if (Date.now() - (c.lastG || 0) < 2000) return send(c, { t: "friend_msg", msg: "slow down lol (2 sec)" });
      c.lastG = Date.now();
      await loadGlobalChat();
      const st = await getStats(c.name);
      const m = { from: c.name, to: "", text, sticker, at: Date.now(), badge: badgeOf(c.name), owner: isOwnerName(c.name), pfp: PFPS.includes(st.pfp) ? st.pfp : "bf" };
      await addGlobalChat(m);
      for (const s of sessions.values()) if (s.inGlobal) send(s, { t: "gchat_msg", msg: m });
      break;
    }
    case "admin_gchat_clear": {
      if (!isOwnerName(c.name)) return send(c, { t: "error", msg: "not allowed" });
      globalChat.length = 0;
      if (DB_URL && DB_TOKEN) await db(["DEL", "gchat"]).catch(() => {});
      for (const s of sessions.values()) if (s.inGlobal) send(s, { t: "gchat_history", msgs: [] });
      send(c, { t: "admin_done", msg: "global chat cleared" });
      break;
    }
    // ---------- friends
    case "friends_list":
      await sendFriends(c);
      break;
    case "friend_add": {
      const key = String(msg.name || "").toLowerCase();
      if (!key || key === c.name.toLowerCase()) return send(c, { t: "friend_msg", msg: "cant add urself lol" });
      if (!allAccounts.has(key) && !(await getAccount(key))) return send(c, { t: "friend_msg", msg: "theres no account called " + msg.name });
      const list = await getFriends(c.name);
      if (list.includes(key)) return send(c, { t: "friend_msg", msg: displayName(key) + " is already ur friend" });
      if (list.length >= 100) return send(c, { t: "friend_msg", msg: "friends list is full (100)" });
      const target = findSessionByName(key);
      // they already asked me: just be friends
      if ((await getRequests(c.name)).includes(key)) {
        await makeFriends(c.name, key);
        send(c, { t: "friend_msg", msg: "ur friends with " + displayName(key) + " now!" });
        await sendFriends(c);
        await sendRequests(c);
        if (target) {
          send(target, { t: "friend_msg", msg: c.name + " accepted ur friend request!" });
          await sendFriends(target);
        }
        break;
      }
      if ((await getStats(key)).noFR) return send(c, { t: "friend_msg", msg: displayName(key) + " turned off friend requests" });
      if ((await getRequests(key)).length >= 50) return send(c, { t: "friend_msg", msg: displayName(key) + " has too many requests rn" });
      await setRequest(key, c.name, true);
      send(c, { t: "friend_msg", msg: "friend request sent to " + displayName(key) + "!" });
      if (target) {
        send(target, { t: "friend_request", from: c.name });
        await sendFriends(target);
      }
      break;
    }
    case "friend_requests":
      await sendRequests(c);
      break;
    case "friend_accept": {
      const key = String(msg.name || "").toLowerCase();
      if (!(await getRequests(c.name)).includes(key)) return send(c, { t: "friend_msg", msg: "that request is gone" });
      if ((await getFriends(c.name)).length >= 100) return send(c, { t: "friend_msg", msg: "friends list is full (100)" });
      await makeFriends(c.name, key);
      send(c, { t: "friend_msg", msg: "ur friends with " + displayName(key) + " now!" });
      await sendFriends(c);
      await sendRequests(c);
      const target = findSessionByName(key);
      if (target) {
        send(target, { t: "friend_msg", msg: c.name + " accepted ur friend request!" });
        await sendFriends(target);
      }
      break;
    }
    case "friend_decline": {
      await setRequest(c.name, String(msg.name || ""), false);
      send(c, { t: "friend_msg", msg: "declined " + msg.name });
      await sendRequests(c);
      await sendFriends(c);
      break;
    }
    case "friend_remove": {
      // unfriending works both ways
      await setFriend(c.name, String(msg.name || ""), false);
      await setFriend(String(msg.name || ""), c.name, false);
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
    // ---------- chat
    case "chat_send": {
      const toKey = String(msg.to || "").toLowerCase();
      if (!toKey || toKey === c.name.toLowerCase() || !allAccounts.has(toKey)) return send(c, { t: "friend_msg", msg: "cant send to that account" });
      const text = String(msg.text || "").replace(/[\r\n]+/g, " ").slice(0, 200).trim();
      const sticker = typeof msg.sticker === "string" && STICKER_RE.test(msg.sticker) && !msg.sticker.includes("..") ? msg.sticker : null;
      if (!text && !sticker) return;
      const mute = await getKV("mutes", c.name.toLowerCase());
      if (mute && (!mute.until || Date.now() < mute.until)) return send(c, { t: "friend_msg", msg: "ur muted" + (mute.until ? " for " + timeLeft(mute.until) : "") });
      const last = c.lastChat || 0;
      if (Date.now() - last < 600) return send(c, { t: "friend_msg", msg: "slow down lol" });
      c.lastChat = Date.now();
      const m = { from: c.name, to: displayName(toKey), text, sticker, at: Date.now() };
      await chatStore(c.name, toKey, m);
      send(c, { t: "chat_msg", msg: m });
      const target = findSessionByName(toKey);
      if (target) send(target, { t: "chat_msg", msg: m });
      await addUnread(toKey, c.name, 1);
      break;
    }
    case "chat_history": {
      const withKey = String(msg.with || "").toLowerCase();
      if (!withKey) return;
      await addUnread(c.name, withKey, 0); // opened the chat = read
      send(c, { t: "chat_history", with: displayName(withKey), msgs: await chatHistory(c.name, withKey) });
      break;
    }
    case "chat_read":
      await addUnread(c.name, String(msg.with || ""), 0);
      break;
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

// when each account was last online (owner info)
const lastSeenMap = new Map();
function markSeen(name) {
  const key = String(name).toLowerCase();
  lastSeenMap.set(key, Date.now());
  if (DB_URL && DB_TOKEN) db(["HSET", "seen", key, String(Date.now())]).catch(() => {});
}
async function getSeen(key) {
  if (lastSeenMap.has(key)) return lastSeenMap.get(key);
  if (!DB_URL || !DB_TOKEN) return null;
  try {
    const v = await db(["HGET", "seen", key]);
    return v ? Number(v) : null;
  } catch {
    return null;
  }
}

function removeSession(c) {
  if (!sessions.has(c.sid)) return;
  markSeen(c.name);
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
    if (await isBanned(realName)) return { msgs: [{ t: "error", code: "banned", msg: await banMessage(realName.toLowerCase()) }] };
    // same account logging in twice: kick the old one
    for (const old of [...sessions.values()])
      if (old.name.toLowerCase() === realName.toLowerCase()) {
        old.hostCode = null; // room stays, the new login can take it back
        leaveQueue(old);
        leaveRoom(old);
        old.kickMsg = "ur account logged in on another device";
      }
    c = { sid: newId(), name: realName, room: null, inbox: [], lastSeen: Date.now(), joined: Date.now(), device: String(hello.device || "").slice(0, 12), tokenHash: sha(hello.token) };
    sessions.set(c.sid, c);
    {
      let n = 0;
      for (const s of sessions.values()) if (!s.kickMsg) n++;
      const td = todayStats();
      if (n > td.peak) td.peak = n;
    }
    send(c, { t: "welcome", name: c.name, online: sessions.size, owner: isOwnerName(c.name), maintenance: !!serverConfig.maintenance });
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

  if (c.endAfter) {
    // changed their name: give them the reply, then they reconnect as the new name
    sessions.delete(c.sid);
    return { msgs: c.inbox.splice(0), now: Date.now() };
  }
  return { sid: c.sid, msgs: c.inbox.splice(0), now: Date.now() };
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

  // iPhone: the game cant open shortcuts:// links itself, so it opens this page and the page opens the Shortcut
  if (req.method === "GET" && req.url.startsWith("/sc")) {
    const job = new URL(req.url, "http://x").searchParams.get("job") || "";
    if (!/^[A-Za-z0-9_.-]{1,140}$/.test(job)) {
      res.writeHead(400);
      return res.end("bad job");
    }
    const host = String(req.headers.host || "");
    const proto = req.headers["x-forwarded-proto"] || (host.startsWith("localhost") || host.startsWith("127.") ? "http" : "https");
    const file = `${proto}://${host}/modfile?job=${encodeURIComponent(job)}`;
    const sc = "shortcuts://run-shortcut?name=" + encodeURIComponent("FNF Online Install") + "&input=text&text=" + encodeURIComponent(file);
    const esc = sc.replace(/&/g, "&amp;").replace(/"/g, "&quot;");
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
    res.end(`<!DOCTYPE html><html><head><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="0;url=${esc}"><title>FNF Online install</title>
<style>body{font-family:-apple-system,sans-serif;background:#1b1b2f;color:#fff;text-align:center;padding:40px 20px}
a{display:inline-block;margin-top:24px;padding:18px 28px;background:#7c5cff;color:#fff;border-radius:14px;text-decoration:none;font-size:22px;font-weight:700}
p{color:#bbb;font-size:17px}</style></head><body>
<h2>Installing the mod...</h2><p>If Shortcuts didn't open, tap the button. Tap "Open" if iPhone asks.</p>
<a href="${esc}">RUN THE SHORTCUT</a>
<p>No Shortcut yet? <a style="background:#333;font-size:16px;padding:10px 16px" href="https://www.icloud.com/shortcuts/cdbeb3286fbb4c83a82fb8dcd0f135d2">Get it here</a></p>
<script>location.href=${JSON.stringify(sc)};</script></body></html>`);
    return;
  }

  // player count for the online menu
  if (req.method === "GET" && req.url.startsWith("/online")) {
    let online = 0;
    for (const c of sessions.values()) if (!c.kickMsg) online++;
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    const ann = lastAnnounce && Date.now() - lastAnnounce.at < 10 * 60000 ? lastAnnounce : null;
    res.end(
      JSON.stringify({
        online,
        searching: queue.length,
        matches: rooms.size,
        announce: ann ? ann.msg : null,
        announceId: ann ? ann.id : null,
        maintenance: !!serverConfig.maintenance,
        news: serverConfig.news,
      })
    );
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
loadConfig();

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
