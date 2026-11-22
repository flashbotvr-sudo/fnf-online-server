// FNF Online matchmaking + relay server (HTTP version)
// Works on normal web hosts like Render, and locally for testing.
// No npm packages needed. Run: node server.js   (PORT env var to change port, default 7777)
//
// The game sends POST /poll with {sid, out:[messages]} and gets back {sid, msgs:[messages]}.
// Open the server address in a browser to check if its up.

const http = require("http");

const PORT = Number(process.env.PORT || 7777);
const PROTOCOL = 2;
const SESSION_TIMEOUT_MS = 20000; // no poll for this long = player is gone
const SONGS = ["bopeebo", "fresh", "dadbattle"]; // songs matches can pick, change whenever

const sessions = new Map(); // sid -> client
let queue = []; // sids waiting for a match
const rooms = new Map(); // roomId -> { a, b, song }
let shuttingDown = false;

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

function tryMatch() {
  while (queue.length >= 2) {
    const a = sessions.get(queue.shift());
    const b = sessions.get(queue.shift());
    if (!a || !b) continue;
    const roomId = "r" + newId();
    const song = SONGS[Math.floor(Math.random() * SONGS.length)];
    rooms.set(roomId, { a: a.sid, b: b.sid, song });
    a.room = b.room = roomId;
    // a plays as bf, b as dad
    send(a, { t: "match", room: roomId, song, side: "bf", opponent: b.name });
    send(b, { t: "match", room: roomId, song, side: "dad", opponent: a.name });
    console.log(`match: ${a.name} vs ${b.name} on ${song}`);
  }
  broadcastQueueCount();
}

function cleanName(n) {
  // same rules as the game: 3-16 letters, numbers or symbols, no spaces
  n = String(n || "").replace(/[^\x21-\x7E]/g, "").slice(0, 16);
  return n.length >= 3 ? n : "guest" + Math.floor(Math.random() * 9999);
}

function handle(c, msg) {
  if (!msg || typeof msg !== "object") return;
  switch (msg.t) {
    case "queue":
      leaveRoom(c);
      if (!queue.includes(c.sid)) queue.push(c.sid);
      tryMatch();
      break;
    case "cancel":
      leaveQueue(c);
      broadcastQueueCount();
      break;
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
  sessions.delete(c.sid);
  broadcastQueueCount();
}

function onPoll(body) {
  if (shuttingDown) return { msgs: [{ t: "server_down" }] };

  const out = Array.isArray(body.out) ? body.out.slice(0, 50) : [];
  let c = body.sid ? sessions.get(body.sid) : null;

  if (!c) {
    // new player, first message has to be hello
    const hello = out.find((m) => m && m.t === "hello");
    if (!hello) return { error: "bad_session", msgs: [{ t: "error", msg: "session expired" }] };
    if (hello.protocol !== PROTOCOL) return { msgs: [{ t: "error", msg: "outdated mod, download the new version" }] };
    c = { sid: newId(), name: cleanName(hello.name), room: null, inbox: [], lastSeen: Date.now() };
    sessions.set(c.sid, c);
    send(c, { t: "welcome", name: c.name, online: sessions.size });
  }

  c.lastSeen = Date.now();
  for (const m of out) if (m && m.t !== "hello") handle(c, m);
  if (!sessions.has(c.sid)) return { msgs: [] }; // said bye

  return { sid: c.sid, msgs: c.inbox.splice(0) };
}

const server = http.createServer((req, res) => {
  if (req.method === "POST" && req.url.startsWith("/poll")) {
    let data = "";
    req.on("data", (chunk) => {
      data += chunk;
      if (data.length > 65536) req.destroy(); // junk protection
    });
    req.on("end", () => {
      let body = {};
      try {
        body = JSON.parse(data || "{}");
      } catch {
        body = {};
      }
      const result = onPoll(body);
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      res.end(JSON.stringify(result));
    });
    return;
  }

  // anything else: simple status page, handy to check in a browser
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(`FNF Online server is running. players online: ${sessions.size}, in queue: ${queue.length}, matches: ${rooms.size}\n`);
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
