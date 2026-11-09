// Hollow Arcade server: serves the website AND runs all online play. One Node process, no per-message billing.
//   /health            -> "ok"
//   /mm/<game>         -> matchmaking queue (pairs players, hands out a room code)
//   /room/<game>-<c>   -> match room (relays every message to the other players)
//   /peer/<game>       -> PeerJS-style registry: players register an id, look each other up and exchange messages
// Everything is plain WebSocket. Memory only: nothing is stored on disk.
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

const ROOT = path.join(path.dirname(fileURLToPath(import.meta.url)), "public");
const PORT = process.env.PORT || 8080;
const ALLOWED = (process.env.ALLOWED_ORIGINS || "").split(",").map((s) => s.trim()).filter(Boolean);
const MAX_MSG = 1024 * 1024;       // bytes per message
const MAX_PEERS = 400;            // per game, total registered ids
const MAX_ROOMS = 400;
const CODE = (n) => Array.from({ length: n }, () => "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"[(Math.random() * 32) | 0]).join("");
const clamp = (n, lo, hi) => Math.min(hi, Math.max(lo, n | 0));
const safe = (s) => String(s || "").replace(/[^A-Za-z0-9_-]/g, "").slice(0, 60);
const ctl = (o) => JSON.stringify(o);

/* ---------- static website ---------- */
const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".json": "application/json", ".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg", ".ico": "image/x-icon", ".txt": "text/plain" };
const server = http.createServer((req, res) => {
  const u = new URL(req.url, "http://x");
  if (u.pathname === "/health") { res.writeHead(200, { "content-type": "text/plain" }); return res.end("ok"); }
  let p = decodeURIComponent(u.pathname);
  if (p.endsWith("/")) p += "index.html";
  const file = path.normalize(path.join(ROOT, p));
  if (!file.startsWith(ROOT)) { res.writeHead(403); return res.end("Forbidden"); }
  fs.stat(file, (e, st) => {
    const send = (f) => {
      const ext = path.extname(f).toLowerCase();
      const headers = { "content-type": MIME[ext] || "application/octet-stream", "cache-control": f === path.join(ROOT, "index.html") ? "no-cache" : "public, max-age=21600" };
      if (/gzip/.test(req.headers["accept-encoding"] || "") && [".html", ".js", ".css", ".json", ".svg"].includes(ext)) {
        headers["content-encoding"] = "gzip";
        res.writeHead(200, headers);
        const rs = fs.createReadStream(f);
        rs.on("error", () => { try { res.end(); } catch (e) {} });
        return rs.pipe(zlib.createGzip({ level: 6 })).pipe(res);
      }
      res.writeHead(200, headers);
      const rs = fs.createReadStream(f);
      rs.on("error", () => { try { res.end(); } catch (e) {} });
      rs.pipe(res);
    };
    if (!e && st.isFile()) return send(file);
    const idx = path.join(ROOT, "index.html");
    if (!path.extname(p) && fs.existsSync(idx)) return send(idx); // unknown page: show the arcade instead of an error
    if (p === "/index.html" && !fs.existsSync(idx)) { res.writeHead(200, { "content-type": "text/plain" }); return res.end("Hollow Arcade game server is running."); }
    res.writeHead(404, { "content-type": "text/plain" }); res.end("Not found");
  });
});
import zlib from "node:zlib";

/* ---------- websocket routing ---------- */
const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_MSG, perMessageDeflate: false });
const rooms = new Map();    // "game-code" -> { size, members:Set<ws> }
const lobbies = new Map();  // game -> Set<ws>   (each ws.meta = {size,name,t})
const peers = new Map();    // game -> Map<id, ws>

server.on("upgrade", (req, socket, head) => {
  const u = new URL(req.url, "http://x");
  const origin = req.headers.origin;
  if (ALLOWED.length && origin && !ALLOWED.includes(origin)) { socket.write("HTTP/1.1 403 Forbidden\r\n\r\n"); return socket.destroy(); }
  const [kind, id] = u.pathname.split("/").filter(Boolean);
  if (!["mm", "room", "peer"].includes(kind) || !safe(id)) { socket.write("HTTP/1.1 404 Not Found\r\n\r\n"); return socket.destroy(); }
  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.isAlive = true;
    ws.on("pong", () => { ws.isAlive = true; });
    ws.on("error", () => {});
    if (kind === "mm") return lobby(ws, safe(id), u.searchParams);
    if (kind === "room") return room(ws, safe(id), u.searchParams);
    return peer(ws, safe(id), u.searchParams);
  });
});
const send = (ws, o) => { try { if (ws.readyState === 1) ws.send(typeof o === "string" ? o : ctl(o)); } catch (e) {} };
const isPing = (m) => m.length === 4 && String(m) === "ping";

/* ---- match rooms (relay) ---- */
function room(ws, key, q) {
  const mode = q.get("mode") || "any";
  let r = rooms.get(key);
  if (!r || r.members.size === 0) {
    if (!r && rooms.size >= MAX_ROOMS) { send(ws, { _s: "full" }); return ws.close(1000, "full"); }
    r = { size: clamp(parseInt(q.get("size")) || 2, 2, 8), members: new Set() };
    rooms.set(key, r);
  }
  let reject = null;
  if (r.members.size >= r.size) reject = "full";
  else if (mode === "create" && r.members.size > 0) reject = "taken";
  else if (mode === "join" && r.members.size === 0) reject = "notfound";
  if (reject) { if (!r.members.size) rooms.delete(key); send(ws, { _s: reject }); return ws.close(1000, reject); }
  const hasHost = [...r.members].some((s) => s.role === "host");
  ws.role = hasHost ? "guest" : "host";
  r.members.add(ws);
  const n = r.members.size;
  send(ws, { _s: "role", role: ws.role, peer: n > 1, n, size: r.size });
  for (const s of r.members) if (s !== ws) send(s, { _s: "peer", on: true, n, size: r.size });
  ws.on("message", (m, isBin) => {
    if (!isBin && isPing(m)) return send(ws, "pong");
    for (const s of r.members) if (s !== ws && s.readyState === 1) { try { s.send(m, { binary: isBin }); } catch (e) {} }
  });
  ws.on("close", () => {
    r.members.delete(ws);
    for (const s of r.members) send(s, { _s: "peer", on: r.members.size > 1, n: r.members.size });
    if (r.members.size === 0) rooms.delete(key);
  });
}

/* ---- matchmaking queue ---- */
function lobby(ws, game, q) {
  ws.meta = { size: clamp(parseInt(q.get("size")) || 2, 2, 8), name: (q.get("name") || "Player").slice(0, 20), t: Date.now() };
  let set = lobbies.get(game); if (!set) lobbies.set(game, (set = new Set()));
  set.add(ws);
  const pump = () => {
    const waiting = [...set].filter((s) => s.readyState === 1).sort((a, b) => a.meta.t - b.meta.t);
    const bySize = {};
    for (const w of waiting) (bySize[w.meta.size] = bySize[w.meta.size] || []).push(w);
    for (const sz of Object.keys(bySize)) {
      const qq = bySize[sz], need = +sz;
      while (qq.length >= need) {
        const group = qq.splice(0, need), code = CODE(6), names = group.map((g) => g.meta.name);
        for (const g of group) { set.delete(g); send(g, { _s: "match", room: code, size: need, names }); try { g.close(1000, "matched"); } catch (e) {} }
      }
      for (const w of qq) send(w, { _s: "queue", n: qq.length, need });
    }
  };
  ws.on("message", (m) => { if (isPing(m)) send(ws, "pong"); });
  ws.on("close", () => { set.delete(ws); pump(); });
  pump();
}

/* ---- PeerJS-style registry ----
   client -> server: {t:"reg",id}  {t:"conn",to,cid,meta}  {t:"d",to,cid,d}  {t:"x",to,cid}
   server -> client: {t:"ready",id} {t:"idtaken"} {t:"conn",from,cid,meta} {t:"d",from,cid,d} {t:"x",from,cid} {t:"nopeer",to,cid} */
function peer(ws, game, q) {
  let map = peers.get(game); if (!map) peers.set(game, (map = new Map()));
  ws.pid = null;
  ws.on("message", (raw, isBin) => {
    if (isBin) return;
    const s = String(raw);
    if (s === "ping") return send(ws, "pong");
    let m; try { m = JSON.parse(s); } catch (e) { return; }
    if (!m || typeof m !== "object") return;
    if (m.t === "reg") {
      if (ws.pid) return;
      const id = String(m.id || "").replace(/[^A-Za-z0-9_-]/g, "").slice(0, 64) || ("p" + CODE(10));
      if (map.has(id) && map.get(id).readyState === 1) return send(ws, { t: "idtaken" });
      if (map.size >= MAX_PEERS) return send(ws, { t: "idtaken", why: "busy" });
      ws.pid = id; map.set(id, ws);
      return send(ws, { t: "ready", id });
    }
    if (!ws.pid) return;
    const to = map.get(String(m.to || ""));
    if (m.t === "conn" || m.t === "ack" || m.t === "d" || m.t === "x") {
      if (!to || to.readyState !== 1) { if (m.t === "conn") send(ws, { t: "nopeer", to: m.to, cid: m.cid }); else if (m.t === "d") send(ws, { t: "x", from: m.to, cid: m.cid }); return; }
      m.from = ws.pid; delete m.to; try { to.send(JSON.stringify(m)); } catch (e) {}
    }
  });
  ws.on("close", () => {
    if (ws.pid && map.get(ws.pid) === ws) map.delete(ws.pid);
    // tell everyone who might have a link to this id that it is gone
    for (const [, o] of map) send(o, { t: "gone", id: ws.pid });
  });
}

/* ---------- keep connections honest (drops dead sockets, also keeps free hosts awake while games run) ---------- */
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { try { ws.terminate(); } catch (e) {} continue; } ws.isAlive = false; try { ws.ping(); } catch (e) {} } }, 30000);

server.listen(PORT, () => console.log("Hollow Arcade server on :" + PORT));
