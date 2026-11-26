// Word Ladder multiplayer room server (Hugging Face Spaces, free tier).
// Protocol-compatible with the Cloudflare Room Durable Object: same messages,
// same endpoints, so the client can switch base URL with zero logic changes.
// Rooms are in-memory with a TTL sweeper (24h live rooms, 14d async games).

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 7860;
const ROOM_TTL_MS = 24 * 60 * 60 * 1000;
const ASYNC_TTL_MS = 14 * 24 * 60 * 60 * 1000;
const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const ALLOWED_ORIGINS = new Set([
  'https://word-ladder-v2.avishorwork.workers.dev',
  'https://word-ladder.avishorwork.workers.dev',
]);

function makeCode(n) {
  const bytes = crypto.randomBytes(n);
  let s = '';
  for (let i = 0; i < n; i++) s += CODE_ALPHABET[bytes[i] % CODE_ALPHABET.length];
  return s;
}

// ---- Room (in-memory mirror of the Durable Object) ----
class Room {
  constructor(code) {
    this.code = code;
    this.initialized = false;
    this.players = []; // {id,name,seq,host,ws,word,moves,path}
    this.gameState = 'lobby'; // lobby | racing | done (live), waiting | playing | done (async)
    this.puzzle = null;
    this.winner = null;
    this.mode = 'race'; // race | coop | async
    this.path = [];
    this.turnPid = null;
    this.nextSeq = 0;
    this.creatorPid = null;
    this.settings = null; // {len,diff,lang} chosen by host
    this.lastActivity = Date.now();
  }
  randomTurn() {
    const open = this.players.filter(p => p.ws);
    const pool = open.length ? open : this.players;
    if (!pool.length) return null;
    return pool[Math.floor(Math.random() * pool.length)].id;
  }
  touch() { this.lastActivity = Date.now(); }
  expired() { return Date.now() - this.lastActivity > (this.mode === 'async' ? ASYNC_TTL_MS : ROOM_TTL_MS); }
  asyncInit(pid, name, puzzle) {
    this.initialized = true;
    this.mode = 'async';
    this.creatorPid = pid;
    this.players = [{ id: pid, name, seq: 1, host: true, ws: null, word: '', moves: 0, path: null }];
    this.nextSeq = 1;
    this.puzzle = { s: String(puzzle.s).toLowerCase(), e: String(puzzle.e).toLowerCase(), o: puzzle.o | 0 };
    this.path = [this.puzzle.s];
    this.turnPid = pid;
    this.gameState = 'waiting';
    this.touch();
  }
  asyncJoin(pid, name) {
    let player = this.players.find(p => p.id === pid);
    if (!player) {
      if (this.players.length >= 2) return { err: [403, { error: 'That game already has 2 players.' }] };
      player = { id: pid, name, seq: ++this.nextSeq, host: false, ws: null, word: '', moves: 0, path: null };
      this.players.push(player);
      if (this.gameState === 'waiting' && this.players.length === 2) {
        this.gameState = 'playing';
        this.turnPid = Math.random() < 0.5 ? this.players[0].id : this.players[1].id;
      }
    } else player.name = name;
    this.touch();
    return { state: this.asyncState(pid) };
  }
  asyncMove(pid, wordIn) {
    if (this.gameState !== 'playing')
      return { err: [409, { error: this.gameState === 'waiting' ? 'Waiting for your partner to join.' : 'This game is finished.', state: this.asyncState(pid) }] };
    const player = this.players.find(p => p.id === pid);
    if (!player) return { err: [403, { error: 'Join the game first.' }] };
    if (pid !== this.turnPid) return { err: [409, { error: 'Not your turn yet.', state: this.asyncState(pid) }] };
    const w = String(wordIn || '').toLowerCase().slice(0, 12);
    const cur = this.path[this.path.length - 1];
    const end = this.puzzle.e;
    const wa = Array.from(w), ca = Array.from(cur), ea = Array.from(end);
    if (!wa.length || wa.length !== ca.length) return { err: [400, { error: 'bad word' }] };
    let diff = 0, breaksLock = false;
    for (let i = 0; i < wa.length; i++) {
      if (wa[i] !== ca[i]) diff++;
      if (ca[i] === ea[i] && wa[i] !== ea[i]) breaksLock = true;
    }
    if (diff !== 1 || breaksLock) return { err: [409, { error: 'Change exactly one unlocked letter.', state: this.asyncState(pid) }] };
    this.path.push(w);
    player.word = w; player.moves = (player.moves || 0) + 1;
    if (w === end) { this.gameState = 'done'; this.winner = 'both'; }
    else { const other = this.players.find(p => p.id !== pid); this.turnPid = other ? other.id : pid; }
    this.touch();
    return { state: this.asyncState(pid) };
  }
  asyncState(pid) {
    return {
      t: 'async', state: this.gameState, mode: 'async', puzzle: this.puzzle, path: this.path,
      turn: this.turnPid, players: this.players.map(p => ({ id: p.id, name: p.name })),
      yourTurn: this.turnPid === pid, inRoom: this.players.some(p => p.id === pid),
      winner: this.winner, moves: this.path.length - 1, lastActivity: this.lastActivity,
    };
  }
  sendTo(player, msg) { if (player.ws && player.ws.readyState === 1) { try { player.ws.send(JSON.stringify(msg)); } catch (e) {} } }
  broadcast(msg, exceptId) {
    const s = JSON.stringify(msg);
    for (const p of this.players) if (p.ws && p.ws.readyState === 1 && p.id !== exceptId) { try { p.ws.send(s); } catch (e) {} }
  }
  sendRoomState() {
    this.broadcast({
      t: 'room',
      players: this.players.filter(p => p.ws).map(p => ({ id: p.id, name: p.name, host: p.host })),
      state: this.gameState, mode: this.mode,
      settings: this.settings,
      puzzle: this.gameState === 'racing' ? this.puzzle : null,
      path: this.mode === 'coop' && this.gameState === 'racing' ? this.path : null,
      turn: this.mode === 'coop' ? this.turnPid : null,
      progress: this.gameState === 'racing' ? Object.fromEntries(this.players.map(p => [p.id, { word: p.word, moves: p.moves, path: p.path || null }])) : null,
    });
  }
  joinWS(ws, pid, name) {
    const isOpen = p => p.ws && p.ws.readyState === 1;
    if (this.mode === 'async') {
      ws.send(JSON.stringify({ t: 'error', msg: 'That code is a Take Turns game - open it from the list on the home screen.', fatal: true }));
      ws.close();
      return;
    }
    let player = this.players.find(p => p.id === pid);
    if (player) {
      if (player.ws && player.ws !== ws) { try { player.ws.send(JSON.stringify({ t: 'replaced' })); } catch (e) {} try { player.ws.close(); } catch (e) {} }
      player.ws = ws; player.name = name;
    } else {
      if (this.gameState !== 'racing') this.players = this.players.filter(isOpen);
      if (this.players.filter(isOpen).length >= 2) {
        ws.send(JSON.stringify({ t: 'error', msg: 'That room is full (2 players).', fatal: true }));
        ws.close();
        return;
      }
      player = { id: pid, name, seq: ++this.nextSeq, host: false, ws, word: '', moves: 0, path: null };
      this.players.push(player);
    }
    player.lastMsg = Date.now();
    const connected = this.players.filter(p => p.ws);
    const creator = connected.find(p => p.id === this.creatorPid);
    const hostP = creator || connected.slice().sort((a, b) => a.seq - b.seq)[0];
    this.players.forEach(p => { p.host = p === hostP; });
    this.wire(ws, player);
    this.sendRoomState();
    this.touch();
  }
  wire(ws, player) {
    ws.on('message', data => {
      player.lastMsg = Date.now();
      let m; try { m = JSON.parse(data); } catch (e) { return; }
      try { this.onMessage(player, m); } catch (e) { /* one bad message must never take the server down */ }
    });
    const drop = () => {
      if (player.ws === ws) player.ws = null;
      this.touch();
      if (this.players.some(p => p.ws)) this.broadcast({ t: 'player_left', id: player.id }, player.id);
    };
    ws.on('close', drop);
    ws.on('error', drop);
  }
  onMessage(player, m) {
    switch (m.t) {
      case 'ping': this.sendTo(player, { t: 'pong', echo: m.ts }); return;
      case 'settings': {
        if (!player.host || this.gameState !== 'lobby') return;
        const s = m.settings || {};
        this.settings = { len: (s.len | 0) || 4, diff: String(s.diff || 'easy'), lang: String(s.lang || 'en').slice(0, 4) };
        this.broadcast({ t: 'settings', settings: this.settings }, player.id);
        this.touch();
        return;
      }
      case 'start': {
        if (!player.host || this.players.length < 2) return;
        if (!m.puzzle || typeof m.puzzle.s !== 'string' || typeof m.puzzle.e !== 'string') return;
        this.puzzle = { s: m.puzzle.s.toLowerCase(), e: m.puzzle.e.toLowerCase(), o: m.puzzle.o | 0 };
        this.gameState = 'racing'; this.winner = null;
        this.players.forEach(p => { p.word = this.puzzle.s; p.moves = 0; p.path = null; });
        this.path = [this.puzzle.s];
        this.turnPid = this.mode === 'coop' ? this.randomTurn() : player.id;
        this.startedAt = Date.now();
        this.broadcast({ t: 'started', puzzle: this.puzzle, mode: this.mode, turn: this.turnPid, startAt: Date.now() + 3200 });
        this.touch();
        return;
      }
      case 'progress': {
        if (this.gameState !== 'racing') return;
        player.word = String(m.word || '').slice(0, 8);
        player.moves = m.moves | 0;
        if (Array.isArray(m.path)) player.path = m.path.slice(0, 60).map(w => String(w).slice(0, 12));
        this.broadcast({ t: 'peer_progress', id: player.id, word: player.word, moves: player.moves, path: player.path || null }, player.id);
        return;
      }
      case 'move': {
        if (this.mode !== 'coop' || this.gameState !== 'racing') return;
        if (player.id !== this.turnPid) { this.sendTo(player, { t: 'notice', msg: 'Not your turn yet.' }); return; }
        const w = String(m.word || '').toLowerCase().slice(0, 8);
        const cur = this.path[this.path.length - 1];
        const end = this.puzzle.e;
        if (!/^[a-z]+$/.test(w) || w.length !== cur.length) return;
        let diff = 0, breaksLock = false;
        for (let i = 0; i < w.length; i++) {
          if (w[i] !== cur[i]) diff++;
          if (cur[i] === end[i] && w[i] !== end[i]) breaksLock = true;
        }
        if (diff !== 1 || breaksLock) { this.sendTo(player, { t: 'notice', msg: 'Change exactly one unlocked letter.' }); return; }
        this.path.push(w);
        const moves = this.path.length - 1;
        if (w === end) {
          this.gameState = 'done'; this.winner = 'both';
          this.broadcast({ t: 'coop_win', path: this.path, moves, ms: Date.now() - (this.startedAt || Date.now()) });
          this.touch();
          return;
        }
        const other = this.players.find(p => p.id !== player.id && p.ws);
        this.turnPid = other ? other.id : player.id;
        this.broadcast({ t: 'coop_state', path: this.path, turn: this.turnPid, by: player.name });
        this.touch();
        return;
      }
      case 'undo': {
        if (this.mode !== 'coop' || this.gameState !== 'racing' || this.path.length <= 1) return;
        this.path.pop();
        this.turnPid = player.id;
        this.broadcast({ t: 'coop_state', path: this.path, turn: this.turnPid, by: player.name });
        this.touch();
        return;
      }
      case 'restart': {
        if (this.mode !== 'coop' || this.gameState !== 'racing') return;
        this.path = [this.puzzle.s];
        this.turnPid = player.id;
        this.broadcast({ t: 'coop_state', path: this.path, turn: this.turnPid, by: player.name });
        this.touch();
        return;
      }
      case 'finish': {
        if (this.gameState !== 'racing' || this.winner) return;
        this.winner = player.id;
        this.gameState = 'done';
        const path = Array.isArray(m.path) ? m.path.slice(0, 60).map(w => String(w).slice(0, 12)) : null;
        this.broadcast({ t: 'finish', winner: player.id, name: player.name, moves: m.moves | 0, ms: m.ms | 0, path });
        this.touch();
        return;
      }
      case 'rematch': {
        if (this.gameState !== 'done') return;
        if (!m.puzzle || typeof m.puzzle.s !== 'string' || typeof m.puzzle.e !== 'string') return;
        this.puzzle = { s: m.puzzle.s.toLowerCase(), e: m.puzzle.e.toLowerCase(), o: m.puzzle.o | 0 };
        this.gameState = 'racing'; this.winner = null;
        this.players.forEach(p => { p.word = this.puzzle.s; p.moves = 0; p.path = null; });
        this.path = [this.puzzle.s];
        this.turnPid = this.mode === 'coop' ? this.randomTurn() : player.id;
        this.startedAt = Date.now();
        this.broadcast({ t: 'rematch', puzzle: this.puzzle, mode: this.mode, turn: this.turnPid, startAt: Date.now() + 3200 });
        this.touch();
        return;
      }
    }
  }
}

const rooms = new Map(); // code -> Room
function getRoom(code, create) {
  let r = rooms.get(code);
  if (r && r.expired()) { rooms.delete(code); r = null; }
  if (!r && create) { r = new Room(code); rooms.set(code, r); }
  return r;
}
setInterval(() => { for (const [c, r] of rooms) if (r.expired()) rooms.delete(c); }, 10 * 60 * 1000).unref();
// Drop zombie sockets: a backgrounded phone keeps TCP half-open and silently
// misses room updates. Clients ping every 4s, so 25s of silence means dead.
setInterval(() => {
  const now = Date.now();
  for (const [, r] of rooms) {
    if (r.mode === 'async') continue;
    for (const p of r.players) {
      if (p.ws && p.ws.readyState === 1 && p.lastMsg && now - p.lastMsg > 25000) {
        try { p.ws.close(); } catch (e) {}
      }
    }
  }
}, 10000).unref();

// ---- HTTP ----
function cors(req, res) {
  const o = req.headers.origin;
  if (o && ALLOWED_ORIGINS.has(o)) res.setHeader('Access-Control-Allow-Origin', o);
  else res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'content-type');
}
function readBody(req) {
  return new Promise(resolve => {
    let d = '', done = false;
    const finish = () => { if (done) return; done = true; try { resolve(JSON.parse(d || '{}')); } catch (e) { resolve({}); } };
    req.on('data', c => { d += c; if (d.length > 1e5) req.destroy(); });
    req.on('end', finish);
    req.on('close', finish);
    req.on('error', finish);
  });
}
const server = http.createServer(async (req, res) => {
  cors(req, res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  const url = new URL(req.url, 'http://x');
  const json = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };

  if (url.pathname === '/health') { global.__healthHits = (global.__healthHits || 0) + 1; return json(200, { ok: true, rooms: rooms.size, uptime: process.uptime() | 0, hits: global.__healthHits }); }

  if (url.pathname === '/api/room' && req.method === 'POST') {
    const b = await readBody(req);
    let code = makeCode(6);
    for (let tries = 0; tries < 8 && rooms.has(code) && !rooms.get(code).expired(); tries++) code = makeCode(6);
    const r = getRoom(code, true);
    r.initialized = true;
    r.creatorPid = String(b.pid || '').slice(0, 40);
    const m = String(b.mode || 'race').toLowerCase();
    r.mode = m === 'coop' ? 'coop' : 'race';
    const st = b.settings || {};
    r.settings = { len: (st.len | 0) || 4, diff: String(st.diff || 'easy'), lang: String(st.lang || 'en').slice(0, 4) };
    r.touch();
    return json(200, { code });
  }
  if (url.pathname === '/api/room-info' && req.method === 'GET') {
    const code = (url.searchParams.get('room') || '').toUpperCase();
    const r = getRoom(code, false);
    if (!r || !r.initialized) return json(404, { exists: false, error: 'Room not found or expired.' });
    return json(200, { exists: true, mode: r.mode, state: r.gameState, players: r.players.filter(p => p.ws || r.mode === 'async').length, settings: r.settings });
  }
  if (url.pathname === '/api/async/create' && req.method === 'POST') {
    const b = await readBody(req);
    if (!b.puzzle || typeof b.puzzle.s !== 'string' || typeof b.puzzle.e !== 'string') return json(400, { error: 'bad puzzle' });
    let code = makeCode(6);
    for (let tries = 0; tries < 8 && rooms.has(code) && !rooms.get(code).expired(); tries++) code = makeCode(6);
    const r = getRoom(code, true);
    r.asyncInit(String(b.pid || '').slice(0, 40), String(b.name || 'Player').slice(0, 14), b.puzzle);
    return json(200, { code });
  }
  if (url.pathname === '/api/async/join' && req.method === 'POST') {
    const code = (url.searchParams.get('room') || '').toUpperCase();
    const r = getRoom(code, false);
    if (!r || !r.initialized) return json(404, { error: 'Room not found or expired.' });
    const b = await readBody(req);
    const out = r.asyncJoin(String(b.pid || '').slice(0, 40), String(b.name || 'Player').slice(0, 14) || 'Player');
    if (out.err) return json(out.err[0], out.err[1]);
    return json(200, out.state);
  }
  if (url.pathname === '/api/async/state' && req.method === 'GET') {
    const code = (url.searchParams.get('room') || '').toUpperCase();
    const r = getRoom(code, false);
    if (!r || !r.initialized) return json(404, { error: 'Room not found or expired.' });
    return json(200, r.asyncState(url.searchParams.get('pid') || ''));
  }
  if (url.pathname === '/api/async/move' && req.method === 'POST') {
    const code = (url.searchParams.get('room') || '').toUpperCase();
    const r = getRoom(code, false);
    if (!r || !r.initialized) return json(404, { error: 'Room not found or expired.' });
    const b = await readBody(req);
    const out = r.asyncMove(String(b.pid || '').slice(0, 40), b.word);
    if (out.err) return json(out.err[0], out.err[1]);
    return json(200, out.state);
  }
  res.writeHead(404); res.end('not found');
});

// ---- WebSocket ----
const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname !== '/ws') { socket.destroy(); return; }
  const code = (url.searchParams.get('room') || '').toUpperCase();
  const pid = (url.searchParams.get('pid') || '').slice(0, 40);
  const name = (url.searchParams.get('name') || 'Player').slice(0, 14) || 'Player';
  if (!/^[A-Z0-9]{4,8}$/.test(code) || !pid) { socket.destroy(); return; }
  const room = getRoom(code, false);
  wss.handleUpgrade(req, socket, head, ws => {
    if (!room || !room.initialized) {
      ws.send(JSON.stringify({ t: 'error', msg: 'Room not found or expired. Rooms stay open for 24 hours - start a new one.', fatal: true }));
      ws.close();
      return;
    }
    room.joinWS(ws, pid, name);
  });
});

server.listen(PORT, () => console.log('word-ladder rooms listening on :' + PORT));
