// ============================================================================
// Project GogRivals · Official Authoritative Server v0.9.9 (Zero Dependencies)
// - 100% compatible con Render y Node.js nativo (NO requiere 'ws' ni npm install)
// - WebSocket RFC 6455 nativo integrado sobre 'http' + 'crypto' + 'events'
// - Hitbox anatómica exacta de 3 zonas (Cabeza, Torso/Brazos, Piernas) orientada
//   a la rotación del jugador + Lag Compensation (cero "balas mágicas")
// - Protección de Spawn autoritativa de 2.5s en cada aparición y reaparición
// - Sincronizado con cliente v0.9.9 (Optimizacion Movil + PC 60/144/240 FPS + Ultra SSAO Fix)
// - Persistencia completa de Niveles y Top Mundial para todos los jugadores con
//   nombre personalizado, Administradores y Moderadores (sin reset a Nivel 1)
// ============================================================================
'use strict';

const http = require('http');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const fs = require('fs');
const path = require('path');

const VERSION = '0.9.9';
const PORT = parseInt(process.env.PORT || '10000', 10);
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'gogrivals_data.json');

// Credenciales Staff (sincronizadas con el cliente v0.9.9)
const ADMIN_PW = process.env.ADMIN_PW || '32100206';
const MOD_PW = process.env.MOD_PW || '67';
const ADMIN_NAME = '[ADMIN] Willy';
const MOD_NAME = '[MODERADOR] Angel';

// Reglas de partida
const MATCH_LIM = 30;
const MATCH_MIN = 5;
const MATCH_RESTART_MS = 6500;
const HEARTBEAT_MS = 2000;
const TIMEOUT_MS = 6000;
const STALE_NAME_MS = 4000;
const SPAWN_PROT_MS = 2500;
const REGEN_DELAY_MS = 3000;
const REGEN_INTERVAL_MS = 1000;
const REGEN_AMOUNT = 5;
const MAX_SYNC_XP = 1000000000; // Hasta Nivel 1,000,000 (compatible con /up)

// ============================================================================
// MOTOR WEBSOCKET RFC 6455 NATIVO (CERO DEPENDENCIAS EXTERNAS)
// ============================================================================
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_WS_PAYLOAD = 262144;

class NativeWebSocket extends EventEmitter {
  constructor(socket, head) {
    super();
    this.socket = socket;
    this.readyState = 1;
    this._buf = Buffer.alloc(0);
    this._fragOp = 0;
    this._fragChunks = [];
    this._fragSize = 0;
    this._closed = false;

    try {
      socket.setNoDelay(true);
      socket.setKeepAlive(true, 15000);
    } catch (_) {}

    socket.on('data', (chunk) => this._onData(chunk));
    socket.on('end', () => this._finalizeClose());
    socket.on('close', () => this._finalizeClose());
    socket.on('error', (err) => {
      if (this.listenerCount('error') > 0) this.emit('error', err);
      this._finalizeClose();
    });

    if (head && head.length > 0) {
      this._onData(head);
    }
  }

  _sendFrame(opcode, dataBuf) {
    if (this.readyState !== 1 || !this.socket || this.socket.destroyed || !this.socket.writable) return false;
    const payload = Buffer.isBuffer(dataBuf) ? dataBuf : Buffer.from(dataBuf || '');
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.allocUnsafe(2);
      header[0] = 0x80 | (opcode & 0x0f);
      header[1] = len;
    } else if (len < 65536) {
      header = Buffer.allocUnsafe(4);
      header[0] = 0x80 | (opcode & 0x0f);
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.allocUnsafe(10);
      header[0] = 0x80 | (opcode & 0x0f);
      header[1] = 127;
      header.writeUInt32BE(Math.floor(len / 4294967296), 2);
      header.writeUInt32BE(len >>> 0, 6);
    }
    try {
      this.socket.write(Buffer.concat([header, payload]));
      return true;
    } catch (_) {
      this._finalizeClose();
      return false;
    }
  }

  send(data) {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'utf8');
    return this._sendFrame(0x1, buf);
  }

  ping(data) {
    const buf = data ? Buffer.from(data) : Buffer.alloc(0);
    return this._sendFrame(0x9, buf);
  }

  pong(data) {
    const buf = data ? Buffer.from(data) : Buffer.alloc(0);
    return this._sendFrame(0xa, buf);
  }

  close(code, reason) {
    if (this.readyState >= 2) return;
    let payload = Buffer.alloc(0);
    if (typeof code === 'number') {
      const rBuf = reason ? Buffer.from(String(reason), 'utf8').subarray(0, 120) : Buffer.alloc(0);
      payload = Buffer.allocUnsafe(2 + rBuf.length);
      payload.writeUInt16BE(code, 0);
      rBuf.copy(payload, 2);
    }
    this._sendFrame(0x8, payload);
    this.readyState = 2;
    try { this.socket.end(); } catch (_) {}
    setTimeout(() => this.terminate(), 250);
  }

  terminate() {
    if (this.socket && !this.socket.destroyed) {
      try { this.socket.destroy(); } catch (_) {}
    }
    this._finalizeClose();
  }

  _finalizeClose() {
    if (this._closed) return;
    this._closed = true;
    this.readyState = 3;
    this._buf = Buffer.alloc(0);
    this._fragChunks = [];
    this.emit('close');
  }

  _onData(chunk) {
    if (this._closed || !chunk || !chunk.length) return;
    this._buf = this._buf.length ? Buffer.concat([this._buf, chunk]) : chunk;

    if (this._buf.length > MAX_WS_PAYLOAD * 2) {
      this.close(1009, 'Message too big');
      return;
    }

    while (this._buf.length >= 2) {
      const b0 = this._buf[0];
      const b1 = this._buf[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;

      if (len === 126) {
        if (this._buf.length < 4) return;
        len = this._buf.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (this._buf.length < 10) return;
        const hi = this._buf.readUInt32BE(2);
        const lo = this._buf.readUInt32BE(6);
        len = hi * 4294967296 + lo;
        offset = 10;
      }

      if (len > MAX_WS_PAYLOAD) {
        this.close(1009, 'Frame too large');
        return;
      }

      const maskLen = masked ? 4 : 0;
      const frameEnd = offset + maskLen + len;
      if (this._buf.length < frameEnd) return;

      const payload = Buffer.allocUnsafe(len);
      if (masked) {
        const m0 = this._buf[offset];
        const m1 = this._buf[offset + 1];
        const m2 = this._buf[offset + 2];
        const m3 = this._buf[offset + 3];
        const pStart = offset + 4;
        for (let i = 0; i < len; i++) {
          const mk = (i & 3) === 0 ? m0 : (i & 3) === 1 ? m1 : (i & 3) === 2 ? m2 : m3;
          payload[i] = this._buf[pStart + i] ^ mk;
        }
      } else {
        this._buf.copy(payload, 0, offset, offset + len);
      }

      this._buf = this._buf.subarray(frameEnd);

      if (opcode === 0x8) {
        this.close(1000);
        return;
      }
      if (opcode === 0x9) {
        this.pong(payload);
        this.emit('ping', payload);
        continue;
      }
      if (opcode === 0xa) {
        this.emit('pong', payload);
        continue;
      }

      if (opcode === 0x1 || opcode === 0x2) {
        if (fin) {
          this.emit('message', opcode === 0x1 ? payload.toString('utf8') : payload);
        } else {
          this._fragOp = opcode;
          this._fragChunks = [payload];
          this._fragSize = payload.length;
        }
      } else if (opcode === 0x0 && this._fragOp) {
        this._fragSize += payload.length;
        if (this._fragSize > MAX_WS_PAYLOAD) {
          this.close(1009, 'Fragmented message too large');
          return;
        }
        this._fragChunks.push(payload);
        if (fin) {
          const full = Buffer.concat(this._fragChunks);
          const op = this._fragOp;
          this._fragOp = 0;
          this._fragChunks = [];
          this._fragSize = 0;
          this.emit('message', op === 0x1 ? full.toString('utf8') : full);
        }
      }
    }
  }
}

function attachNativeWebSocketServer(httpServer, onConnection) {
  httpServer.on('upgrade', (req, socket, head) => {
    const upgradeHdr = String(req.headers.upgrade || '').toLowerCase();
    const wsKey = req.headers['sec-websocket-key'];
    if (upgradeHdr !== 'websocket' || !wsKey) {
      try {
        socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
        socket.destroy();
      } catch (_) {}
      return;
    }

    const accept = crypto
      .createHash('sha1')
      .update(String(wsKey).trim() + WS_GUID)
      .digest('base64');

    const headers = [
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      'Sec-WebSocket-Accept: ' + accept,
      '\r\n'
    ].join('\r\n');

    try {
      socket.write(headers);
    } catch (_) {
      try { socket.destroy(); } catch (__) {}
      return;
    }

    const ws = new NativeWebSocket(socket, head);
    onConnection(ws, req);
  });
}

// ============================================================================
// DATOS DE ARMAS Y GEOMETRÍA DE MAPAS BLOW / PARK
// ============================================================================
const WEAPONS = [
  { name: 'M4',     d: 25,  r: 90  },
  { name: 'AK47',   d: 35,  r: 120 },
  { name: 'LOCUS',  d: 100, r: 1200 },
  { name: 'P90',    d: 18,  r: 75  },
  { name: 'MAC 10', d: 16,  r: 62  }
];

const SPAWN_POINTS = [
  [-25, -25], [25, 25], [-25, 25], [25, -25],
  [0, -26], [0, 26], [-26, 0], [26, 0]
];

const WALLS = [
  [0, -30.5, 62, 4, 1],
  [0,  30.5, 62, 4, 1],
  [-30.5, 0, 1, 4, 62],
  [ 30.5, 0, 1, 4, 62]
];

const BLOW_B = [
  [0, 0, 6, 1.2, 1.2], [-5, -4, 1.2, 1.2, 6], [5, 4, 1.2, 1.2, 6],
  [0, 14, 5, 1.2, 1.2], [-14, -10, 6, 2.6, 2.4], [14, 10, 6, 2.6, 2.4],
  [-14, 12, 2.4, 2.6, 6], [14, -12, 2.4, 2.6, 6], [0, -18, 6, 2.6, 2.4],
  [-22, 0, 2.4, 2.6, 6], [22, 0, 2.4, 2.6, 6], [-8, 8, 1.5, 1.5, 1.5],
  [9, -7, 1.5, 1.5, 1.5]
];

const PARK_B = [
  [-3.4, -20, 0.9, 9, 0.9], [3.4, -20, 0.9, 9, 0.9], [-17, -6, 6.4, 0.9, 6.4],
  [-17, -6, 0.7, 6, 0.7], [19, -17, 10, 4.6, 7], [-11, 17, 4, 2.4, 2.2],
  [9, -5, 4.4, 2.4, 2.2], [-24, 12, 2.2, 2.4, 4.4], [22, 6, 2.2, 2.4, 4.4],
  [3, 22, 4, 2.4, 2.2], [12, 13.2, 8, 1, 0.4], [12, 18.8, 8, 1, 0.4],
  [8.2, 16, 0.4, 1, 5.2], [15.8, 14.4, 0.4, 1, 1.6], [15.8, 17.6, 0.4, 1, 1.6],
  [-22, -22, 1.6, 14, 1.6], [0, 0, 0.5, 6.5, 0.5], [-10, -10, 0.5, 4.5, 0.5],
  [10, 10, 0.5, 4.5, 0.5], [-10, 10, 0.5, 4.5, 0.5], [6, -12, 1.5, 1.5, 1.5],
  [-6, -14, 1.5, 1.2, 3], [-8, 0, 1.4, 1.2, 1.4], [7, 3, 1.4, 1.2, 1.4],
  [15, -5, 3, 1.2, 1.2], [-14, -18, 1.5, 1.5, 1.5], [2, 12, 3, 1.2, 1.2],
  [-26, -8, 1.5, 1.5, 1.5], [26, -6, 1.4, 1.4, 1.4], [-4, -9, 2.2, 1, 1]
];

function buildAABBs(mapName) {
  const src = WALLS.concat(mapName === 'PARK' ? PARK_B : BLOW_B);
  return src.map(([x, z, w, h, d]) => ({
    x0: x - w / 2, x1: x + w / 2,
    y0: 0,         y1: h,
    z0: z - d / 2, z1: z + d / 2
  }));
}

const MAP_BOXES = {
  BLOW: buildAABBs('BLOW'),
  PARK: buildAABBs('PARK')
};

// ===== Persistencia de Niveles, Top Mundial y Bans =====
let store = {
  profiles: Object.create(null), // lowerKey -> { n, xp, role, forced, ts }
  bans: {
    dev: Object.create(null),
    name: Object.create(null)
  }
};

function loadStore() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      if (raw && typeof raw === 'object') {
        if (raw.profiles && typeof raw.profiles === 'object') store.profiles = raw.profiles;
        if (raw.bans && typeof raw.bans === 'object') {
          store.bans.dev = raw.bans.dev || Object.create(null);
          store.bans.name = raw.bans.name || Object.create(null);
        }
      }
    }
  } catch (_) {}
}

let saveTimer = null;
function saveStoreNow() {
  if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
  try {
    const tmpFile = DATA_FILE + '.tmp';
    fs.writeFileSync(tmpFile, JSON.stringify(store), 'utf8');
    fs.renameSync(tmpFile, DATA_FILE);
  } catch (_) {
    try { fs.writeFileSync(DATA_FILE, JSON.stringify(store), 'utf8'); } catch (__) {}
  }
}

function saveStoreSoon() {
  if (saveTimer) return;
  saveTimer = setTimeout(saveStoreNow, 400);
}

loadStore();
process.on('SIGTERM', () => { saveStoreNow(); process.exit(0); });
process.on('SIGINT', () => { saveStoreNow(); process.exit(0); });

// ===== Utilidades =====
function cleanName(s) {
  return String(s || 'JUGADOR')
    .replace(/[^\wÁÉÍÓÚÑáéíóúñ .\-]/g, '')
    .trim()
    .slice(0, 12) || 'JUGADOR';
}

function normKey(s) {
  return String(s || '').trim().toLowerCase();
}

function isPersistentProfile(name, role) {
  if (role === 'admin' || role === 'mod') return true;
  const k = normKey(name);
  if (!k) return false;
  if (k === normKey(ADMIN_NAME) || k === normKey(MOD_NAME)) return true;
  return k !== 'jugador';
}

function clampXp(v) {
  const n = Math.floor(+v || 0);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.min(MAX_SYNC_XP, n);
}

function levelOf(xp) {
  return Math.floor(Math.max(0, +xp || 0) / 1000) + 1;
}

function safeSend(ws, obj) {
  if (!ws || ws.readyState !== 1) return false;
  try {
    return ws.send(JSON.stringify(obj));
  } catch (_) {
    return false;
  }
}

function activeBan(dev, lowerName) {
  const now = Date.now();
  for (const [bucket, key] of [['dev', dev], ['name', lowerName]]) {
    if (!key) continue;
    const b = store.bans[bucket][key];
    if (!b) continue;
    if (b.until && b.until > 0 && now >= b.until) {
      delete store.bans[bucket][key];
      saveStoreSoon();
      continue;
    }
    return b;
  }
  return null;
}

// ===== Estado de la partida Online =====
const players = new Map();
let nextPlayerNum = 1;
let curMap = 'BLOW';
let matchPlaying = true;
let matchEnded = false;
let matchEndAt = Date.now() + MATCH_MIN * 60000;
let restartTimer = null;

function broadcast(obj, excludeId) {
  const payload = JSON.stringify(obj);
  for (const [id, p] of players.entries()) {
    if (excludeId && id === excludeId) continue;
    if (p.ws && p.ws.readyState === 1) {
      try { p.ws.send(payload); } catch (_) {}
    }
  }
}

function buildRoster() {
  const out = [];
  for (const p of players.values()) {
    if (!p.joined) continue;
    out.push({ id: p.id, n: p.n });
  }
  return out;
}

function buildScoreboard() {
  const sc = {};
  for (const p of players.values()) {
    if (!p.joined) continue;
    sc[p.id] = { n: p.n, k: p.k, d: p.d };
  }
  return sc;
}

function pickSpawnPoint(forId) {
  let best = SPAWN_POINTS[0];
  let bestScore = -1;
  for (const sp of SPAWN_POINTS) {
    let minD = 1e9;
    for (const p of players.values()) {
      if (!p.joined || !p.alive || p.id === forId) continue;
      const d = Math.hypot(p.x - sp[0], p.z - sp[1]);
      if (d < minD) minD = d;
    }
    const score = minD + Math.random() * 6;
    if (score > bestScore) {
      bestScore = score;
      best = sp;
    }
  }
  return best;
}

function getProfile(name, role) {
  if (!isPersistentProfile(name, role)) {
    return { n: 'JUGADOR', xp: 0, role: '', ephemeral: true };
  }
  const k = normKey(name);
  let prof = store.profiles[k];
  if (!prof) {
    prof = { n: name, xp: 0, role: role || '', ts: Date.now() };
    store.profiles[k] = prof;
  } else {
    prof.n = name;
    if (role) prof.role = role;
    prof.ts = Date.now();
  }
  return prof;
}

function mergeProfileXp(name, role, clientXp) {
  if (!isPersistentProfile(name, role)) {
    return { xp: 0, force: false, role: '' };
  }
  const prof = getProfile(name, role);
  const cXp = clampXp(clientXp);
  const sXp = clampXp(prof.xp);
  if (prof.forced) {
    // Respetar un cambio explícito de /up de Staff una vez y luego reanudar sincronización normal
    prof.forced = false;
    prof.ts = Date.now();
    saveStoreSoon();
    return { xp: sXp, force: true, role: prof.role || role || '' };
  }
  const bestXp = Math.max(sXp, cXp);
  if (bestXp !== sXp || !store.profiles[normKey(name)]) {
    prof.xp = bestXp;
    prof.ts = Date.now();
    saveStoreSoon();
  }
  return { xp: bestXp, force: false, role: prof.role || role || '' };
}

function grantXp(p, delta) {
  if (!p || !(delta > 0)) return;
  if (!isPersistentProfile(p.n, p.role)) return;
  const prof = getProfile(p.n, p.role);
  prof.xp = clampXp((+prof.xp || 0) + delta);
  prof.ts = Date.now();
  p.xp = prof.xp;
  saveStoreSoon();
  safeSend(p.ws, { t: 'level', xp: p.xp });
}

// ============================================================================
// HITBOX ANATÓMICA EXACTA DE 3 ZONAS ORIENTADAS AL CUERPO (CERO BALAS MÁGICAS)
// Coincide 1:1 con el modelo 3D del soldado en index.html (mkBot + animSoldier)
// ============================================================================
function rayBoxDist(ox, oy, oz, dx, dy, dz, b, maxDist) {
  let tmin = 0;
  let tmax = maxDist;

  if (Math.abs(dx) < 1e-8) {
    if (ox < b.x0 || ox > b.x1) return -1;
  } else {
    const inv = 1 / dx;
    let t1 = (b.x0 - ox) * inv;
    let t2 = (b.x1 - ox) * inv;
    if (t1 > t2) { const tmp = t1; t1 = t2; t2 = tmp; }
    if (t1 > tmin) tmin = t1;
    if (t2 < tmax) tmax = t2;
    if (tmin > tmax) return -1;
  }

  if (Math.abs(dy) < 1e-8) {
    if (oy < b.y0 || oy > b.y1) return -1;
  } else {
    const inv = 1 / dy;
    let t1 = (b.y0 - oy) * inv;
    let t2 = (b.y1 - oy) * inv;
    if (t1 > t2) { const tmp = t1; t1 = t2; t2 = tmp; }
    if (t1 > tmin) tmin = t1;
    if (t2 < tmax) tmax = t2;
    if (tmin > tmax) return -1;
  }

  if (Math.abs(dz) < 1e-8) {
    if (oz < b.z0 || oz > b.z1) return -1;
  } else {
    const inv = 1 / dz;
    let t1 = (b.z0 - oz) * inv;
    let t2 = (b.z1 - oz) * inv;
    if (t1 > t2) { const tmp = t1; t1 = t2; t2 = tmp; }
    if (t1 > tmin) tmin = t1;
    if (t2 < tmax) tmax = t2;
    if (tmin > tmax) return -1;
  }

  return tmin >= 0 ? tmin : -1;
}

function rayMapWallDist(ox, oy, oz, dx, dy, dz, maxDist) {
  const boxes = MAP_BOXES[curMap] || MAP_BOXES.BLOW;
  let best = maxDist;
  for (let i = 0; i < boxes.length; i++) {
    const t = rayBoxDist(ox, oy, oz, dx, dy, dz, boxes[i], best);
    if (t >= 0 && t < best) best = t;
  }
  return best;
}

// Prueba rayo contra las 3 cajas anatómicas orientadas (OBB) del soldado:
//   - Cabeza/Casco:  x ±0.17, y [1.54..1.96], z ±0.17  (Headshot)
//   - Torso/Brazos:  x ±0.29, y [0.92..1.55], z [-0.18..0.22]
//   - Piernas/Botas: x ±0.23, y [0.00..0.94], z [-0.16..0.17]
//   - En Slide:      Cabeza y [0.85..1.24], Cuerpo/Piernas y [0.00..0.88]
function testRayAgainstPose(ox, oy, oz, dx, dy, dz, pose, maxDist, pad) {
  const eps = pad || 0;
  // Rotar rayo al espacio local del soldado (misma convención que updAv: rotY = yw + PI)
  const rotY = (Number.isFinite(pose.yw) ? pose.yw : 0) + Math.PI;
  const c = Math.cos(rotY);
  const s = Math.sin(rotY);

  const rx = ox - pose.x;
  const ry = oy - pose.y;
  const rz = oz - pose.z;

  // Inversa de rotación Y(rotY): lx = rx*c - rz*s, lz = rx*s + rz*c
  const lox = rx * c - rz * s;
  const loy = ry;
  const loz = rx * s + rz * c;

  const ldx = dx * c - dz * s;
  const ldy = dy;
  const ldz = dx * s + dz * c;

  const parts = pose.sl
    ? [
        // Cabeza en slide (ligeramente reclinada hacia atrás en Z local)
        { hd: true,  x0: -0.18 - eps, x1: 0.18 + eps, y0: 0.85 - eps, y1: 1.24 + eps, z0: -0.36 - eps, z1: 0.08 + eps },
        // Torso + Piernas en slide
        { hd: false, x0: -0.28 - eps, x1: 0.28 + eps, y0: 0.00,       y1: 0.88 + eps, z0: -0.34 - eps, z1: 0.44 + eps }
      ]
    : [
        // Cabeza + Casco exacto
        { hd: true,  x0: -0.17 - eps, x1: 0.17 + eps, y0: 1.54 - eps, y1: 1.96 + eps, z0: -0.17 - eps, z1: 0.17 + eps },
        // Torso + Chaleco + Hombros/Brazos exacto
        { hd: false, x0: -0.29 - eps, x1: 0.29 + eps, y0: 0.92 - eps, y1: 1.55 + eps, z0: -0.18 - eps, z1: 0.22 + eps },
        // Piernas + Botas exacto
        { hd: false, x0: -0.23 - eps, x1: 0.23 + eps, y0: 0.00,       y1: 0.94 + eps, z0: -0.16 - eps, z1: 0.17 + eps }
      ];

  let best = null;
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    const limit = best ? best.dist : maxDist;
    const t = rayBoxDist(lox, loy, loz, ldx, ldy, ldz, part, limit);
    if (t >= 0 && (!best || t < best.dist)) {
      best = { dist: t, hd: part.hd };
    }
  }
  return best;
}

function getVictimSnapshots(v, now) {
  const snaps = [{ t: now, x: v.x, y: v.y, z: v.z, yw: v.yw, sl: v.sl }];
  if (Array.isArray(v.hist) && v.hist.length) {
    for (let i = v.hist.length - 1; i >= 0; i--) {
      const s = v.hist[i];
      if (now - s.t > 420) break;
      snaps.push(s);
    }
  }
  return snaps;
}

// ===== Limpieza inmediata y verificación constante de desconexiones =====
function removePlayer(id, reason) {
  const p = players.get(id);
  if (!p) return;
  players.delete(id);

  try {
    if (p.ws) {
      p.ws.removeAllListeners();
      if (p.ws.readyState === 1 || p.ws.readyState === 2) {
        p.ws.terminate();
      }
    }
  } catch (_) {}

  if (p.joined) {
    broadcast({ t: 'vc', from: p.id, sig: 'bye' }, p.id);
    broadcast({ t: 'ro', ro: buildRoster() });
    broadcast({ t: 'sc', sc: buildScoreboard() });
  }
}

function sweepDeadPlayers() {
  const now = Date.now();
  const toDrop = [];

  for (const [id, p] of players.entries()) {
    const ws = p.ws;
    if (!ws || ws.readyState !== 1) {
      toDrop.push([id, 'closed']);
      continue;
    }
    if (now - p.lastSeen > TIMEOUT_MS) {
      toDrop.push([id, 'timeout']);
      continue;
    }
    try { ws.ping(); } catch (_) {}
    safeSend(ws, { t: 'hb', ts: now, hp: p.hp });
  }

  for (const [id, why] of toDrop) {
    removePlayer(id, why);
  }
}

setInterval(sweepDeadPlayers, HEARTBEAT_MS);

// ===== Ciclo de vida de la partida + Regeneración + Sincronización de Protección =====
function endOnlineMatch(winnerId) {
  if (matchEnded) return;
  matchEnded = true;
  matchPlaying = false;

  if (!winnerId) {
    const arr = Array.from(players.values())
      .filter(p => p.joined)
      .sort((a, b) => b.k - a.k || a.d - b.d);
    if (!arr.length) winnerId = 'draw';
    else if (arr.length > 1 && arr[0].k === arr[1].k && arr[0].d === arr[1].d) winnerId = 'draw';
    else winnerId = arr[0].id;
  }

  broadcast({ t: 'end', w: winnerId, sc: buildScoreboard() });

  if (restartTimer) clearTimeout(restartTimer);
  restartTimer = setTimeout(() => {
    restartTimer = null;
    startNewOnlineMatch();
  }, MATCH_RESTART_MS);
}

function startNewOnlineMatch() {
  curMap = curMap === 'BLOW' ? 'PARK' : 'BLOW';
  matchEnded = false;
  matchPlaying = true;
  matchEndAt = Date.now() + MATCH_MIN * 60000;
  const now = Date.now();

  for (const p of players.values()) {
    if (!p.joined) continue;
    const sp = pickSpawnPoint(p.id);
    p.x = sp[0]; p.y = 0; p.z = sp[1];
    p.hp = 100; p.k = 0; p.d = 0;
    p.alive = true;
    p.protUntil = now + SPAWN_PROT_MS;
    p.lastDmgAt = 0; p.lastRegenAt = now;
    p.lastShotAt = 0; p.lastShotQ = -1;
    p.rs = (p.rs || 0) + 1;
    p.hist = [{ t: now, x: p.x, y: 0, z: p.z, yw: p.yw, sl: 0 }];
  }

  const sc = buildScoreboard();
  const remMin = Math.max(0.1, (matchEndAt - now) / 60000);
  for (const p of players.values()) {
    if (!p.joined) continue;
    safeSend(p.ws, { t: 'go', map: curMap, lim: MATCH_LIM, min: remMin, sc });
    safeSend(p.ws, { t: 'sp', x: p.x, y: p.y, z: p.z, hp: 100, rs: p.rs, prot: SPAWN_PROT_MS });
  }
  broadcast({ t: 'ro', ro: buildRoster() });
}

setInterval(() => {
  const now = Date.now();
  if (matchPlaying && !matchEnded) {
    if (players.size > 0 && now >= matchEndAt) {
      endOnlineMatch(null);
      return;
    }
    if (players.size === 0 && now >= matchEndAt) {
      matchEndAt = now + MATCH_MIN * 60000;
    }

    for (const p of players.values()) {
      if (!p.joined || !p.alive) continue;
      if (p.hp > 0 && p.hp < 100 && now - p.lastDmgAt >= REGEN_DELAY_MS) {
        if (now - p.lastRegenAt >= REGEN_INTERVAL_MS) {
          p.lastRegenAt = now;
          p.hp = Math.min(100, p.hp + REGEN_AMOUNT);
          safeSend(p.ws, { t: 'hp', hp: p.hp, s: 1 });
        }
      }
    }
  }
}, 250);

// ===== Procesamiento instantáneo de disparo (Hitbox exacta + Protección 2.5s) =====
function handleShot(shooter, m) {
  if (!matchPlaying || matchEnded || !shooter.joined || !shooter.alive) return;
  const now = Date.now();

  const q = Number.isFinite(+m.q) ? (+m.q | 0) : -1;
  if (q >= 0 && q === shooter.lastShotQ) return;

  const wIdx = (Number.isInteger(m.w) && m.w >= 0 && m.w < WEAPONS.length) ? m.w : (shooter.w || 0);
  const weap = WEAPONS[wIdx] || WEAPONS[0];

  const minInterval = Math.max(32, Math.floor(weap.r * 0.62));
  if (now - shooter.lastShotAt < minInterval) {
    shooter.burstCredit = (shooter.burstCredit || 0) + 1;
    if (shooter.burstCredit > 2) return;
  } else {
    shooter.burstCredit = 0;
  }
  shooter.lastShotAt = now;
  shooter.lastShotQ = q;
  shooter.w = wIdx;

  const o = Array.isArray(m.o) && m.o.length === 3 ? m.o.map(Number) : [shooter.x, shooter.y + (m.sl ? 1.0 : 1.7), shooter.z];
  const d = Array.isArray(m.d) && m.d.length === 3 ? m.d.map(Number) : [0, 0, -1];
  if (!o.every(Number.isFinite) || !d.every(Number.isFinite)) return;
  const dLen = Math.hypot(d[0], d[1], d[2]);
  if (dLen < 1e-4) return;
  const dx = d[0] / dLen, dy = d[1] / dLen, dz = d[2] / dLen;

  let ox = o[0], oy = o[1], oz = o[2];
  const origDelta = Math.hypot(ox - shooter.x, oz - shooter.z);
  if (origDelta > 3.5 || Math.abs(oy - (shooter.y + 1.5)) > 2.5) {
    ox = shooter.x;
    oy = shooter.y + (shooter.sl ? 1.0 : 1.7);
    oz = shooter.z;
  }

  broadcast({
    t: 'shotfx',
    by: shooter.id,
    o: [+ox.toFixed(2), +oy.toFixed(2), +oz.toFixed(2)],
    d: [+dx.toFixed(4), +dy.toFixed(4), +dz.toFixed(4)],
    w: wIdx
  }, shooter.id);

  const wallDist = rayMapWallDist(ox, oy, oz, dx, dy, dz, 125);
  let bestVictim = null;
  let bestDist = wallDist + 0.02;
  let bestHd = false;
  let hitProtectedId = null;

  // 1) Si el cliente impactó sobre la malla 3D del enemigo, validar contra su hitbox anatómica exacta (3 zonas OBB)
  if (m.tg && typeof m.tg === 'string') {
    const cand = players.get(m.tg);
    if (cand && cand.joined && cand.alive && cand.id !== shooter.id) {
      const snaps = getVictimSnapshots(cand, now);
      for (const pose of snaps) {
        const hit = testRayAgainstPose(ox, oy, oz, dx, dy, dz, pose, wallDist + 0.02, 0.025);
        if (hit) {
          if (now < cand.protUntil) {
            hitProtectedId = cand.id;
          } else {
            bestVictim = cand;
            bestDist = hit.dist;
            // Headshot solo si el impacto real cayó en la zona de cabeza
            bestHd = !!(hit.hd && m.hd);
          }
          break;
        }
      }
    }
  }

  // 2) Barrido autoritativo estricto (sin padding ni cono mágico) para disparos en movimiento
  if (!bestVictim && !hitProtectedId) {
    for (const cand of players.values()) {
      if (!cand.joined || !cand.alive || cand.id === shooter.id) continue;
      const snaps = getVictimSnapshots(cand, now);
      for (const pose of snaps) {
        const hit = testRayAgainstPose(ox, oy, oz, dx, dy, dz, pose, bestDist, 0.0);
        if (hit && hit.dist < bestDist) {
          if (now < cand.protUntil) {
            hitProtectedId = cand.id;
            bestDist = hit.dist;
          } else {
            bestVictim = cand;
            bestDist = hit.dist;
            bestHd = hit.hd;
            hitProtectedId = null;
          }
          break;
        }
      }
    }
  }

  if (hitProtectedId && !bestVictim) {
    safeSend(shooter.ws, { t: 'prot', v: hitProtectedId });
    return;
  }

  if (!bestVictim) return;

  const dm = weap.d * (bestHd ? 2 : 1);
  bestVictim.hp = Math.max(0, bestVictim.hp - dm);
  bestVictim.lastDmgAt = now;
  bestVictim.lastRegenAt = now;
  const fatal = bestVictim.hp <= 0;

  safeSend(shooter.ws, {
    t: 'hitc',
    v: bestVictim.id,
    dm,
    hd: bestHd ? 1 : 0,
    hp: bestVictim.hp,
    q,
    fatal: fatal ? 1 : 0
  });

  safeSend(bestVictim.ws, {
    t: 'hp',
    hp: bestVictim.hp,
    by: shooter.id,
    hd: bestHd ? 1 : 0
  });

  if (fatal) {
    bestVictim.alive = false;
    bestVictim.hp = 0;
    bestVictim.d++;
    shooter.k++;

    const sp = pickSpawnPoint(bestVictim.id);
    bestVictim.pendingSpawn = sp;

    grantXp(shooter, bestHd ? 150 : 100);

    safeSend(bestVictim.ws, {
      t: 'die',
      by: shooter.id,
      hd: bestHd ? 1 : 0,
      sx: sp[0],
      sz: sp[1]
    });

    broadcast({
      t: 'sc',
      sc: buildScoreboard(),
      ev: { by: shooter.id, v: bestVictim.id, hd: !!bestHd }
    });

    if (shooter.k >= MATCH_LIM) {
      endOnlineMatch(shooter.id);
    }
  }
}

// ===== Comandos de Moderación Staff =====
function findPlayerByArg(arg) {
  const target = normKey(String(arg || '').replace(/_/g, ' '));
  if (!target) return null;
  for (const p of players.values()) {
    if (!p.joined) continue;
    const pn = normKey(p.n);
    const bare = normKey(p.n.replace(/^\[(ADMIN|MODERADOR)\]\s*/i, ''));
    if (pn === target || bare === target || pn.replace(/\s+/g, '_') === normKey(arg)) return p;
  }
  return null;
}

function handleAdminCmd(p, m) {
  const isAdm = (m.pw === ADMIN_PW && p.role === 'admin');
  const isMod = (m.pw === MOD_PW && p.role === 'mod');
  if (!isAdm && !isMod) return;
  const actor = isAdm ? ADMIN_NAME : MOD_NAME;
  const cmd = String(m.c || '').toLowerCase();

  if (cmd === 'kick') {
    const tgt = findPlayerByArg(m.n);
    if (!tgt) return safeSend(p.ws, { t: 'chat', id: 'SYS', n: 'SERVIDOR', m: 'Jugador no encontrado: ' + m.n });
    const r = String(m.r || 'Expulsado por staff').slice(0, 80);
    safeSend(tgt.ws, { t: 'admact', k: 'kick', a: actor, r });
    setTimeout(() => removePlayer(tgt.id, 'kicked'), 120);
    broadcast({ t: 'chat', id: 'SYS', n: 'SERVIDOR', m: `${tgt.n} fue expulsado por ${actor} (${r})` });
    return;
  }

  if (cmd === 'ban' || cmd === 'tempban') {
    const tgt = findPlayerByArg(m.n);
    const r = String(m.r || 'Incumplimiento de normas').slice(0, 80);
    const ms = Math.max(0, +m.ms || 0);
    const until = (cmd === 'tempban' && ms > 0) ? (Date.now() + ms) : 0;
    const entry = { until, r, by: actor, ts: m.ts || null };
    const nameKey = normKey(tgt ? tgt.n : String(m.n || '').replace(/_/g, ' '));
    if (nameKey) store.bans.name[nameKey] = entry;
    if (tgt && tgt.dev) store.bans.dev[tgt.dev] = entry;
    saveStoreNow();
    if (tgt) {
      safeSend(tgt.ws, { t: 'admact', k: cmd, a: actor, r, ts: m.ts || null });
      setTimeout(() => removePlayer(tgt.id, 'banned'), 120);
    }
    broadcast({ t: 'chat', id: 'SYS', n: 'SERVIDOR', m: `${tgt ? tgt.n : m.n} fue baneado por ${actor} (${r})` });
    return;
  }

  if (cmd === 'unban') {
    const key = normKey(String(m.n || '').replace(/_/g, ' '));
    let removed = false;
    if (store.bans.name[key]) { delete store.bans.name[key]; removed = true; }
    for (const dk of Object.keys(store.bans.dev)) {
      if (normKey(dk) === key) { delete store.bans.dev[dk]; removed = true; }
    }
    saveStoreNow();
    safeSend(p.ws, { t: 'chat', id: 'SYS', n: 'SERVIDOR', m: removed ? `Desbaneado: ${m.n}` : `No había ban activo para ${m.n}` });
    return;
  }

  if (cmd === 'up') {
    const lv = Math.max(1, Math.min(1000000, (+m.l | 0) || 1));
    const newXp = (lv - 1) * 1000;
    const rawName = String(m.n || '').replace(/_/g, ' ').trim();
    const tgt = findPlayerByArg(m.n);
    let profName = tgt ? tgt.n : cleanName(rawName);
    let profRole = tgt ? tgt.role : '';
    if (normKey(rawName) === normKey(ADMIN_NAME) || normKey(rawName) === 'willy') { profName = ADMIN_NAME; profRole = 'admin'; }
    else if (normKey(rawName) === normKey(MOD_NAME) || normKey(rawName) === 'angel') { profName = MOD_NAME; profRole = 'mod'; }
    if (!isPersistentProfile(profName, profRole)) {
      return safeSend(p.ws, { t: 'chat', id: 'SYS', n: 'SERVIDOR', m: 'No se puede guardar nivel para el nombre predeterminado JUGADOR.' });
    }
    const prof = getProfile(profName, profRole);
    prof.xp = newXp;
    prof.forced = !tgt; // Si está offline, forzar al reconectar
    prof.ts = Date.now();
    saveStoreNow();
    if (tgt) {
      tgt.xp = newXp;
      safeSend(tgt.ws, { t: 'level', xp: newXp, force: 1 });
    }
    safeSend(p.ws, { t: 'chat', id: 'SYS', n: 'SERVIDOR', m: `Nivel actualizado: ${profName} -> Nivel ${lv}` });
  }
}

// ===== Servidor HTTP (Info, Health, Top Mundial, /sync_profile y /leave) =====
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Cache-Control', 'no-store');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const urlPath = (req.url || '/').split('?')[0];

  if (urlPath === '/' || urlPath === '/info' || urlPath === '/health') {
    sweepDeadPlayers();
    const liveCount = Array.from(players.values()).filter(p => p.joined).length;
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({
      ok: true,
      gog: true,
      version: VERSION,
      map: curMap,
      players: liveCount,
      uptime: Math.floor(process.uptime())
    }));
    return;
  }

  if (urlPath === '/top') {
    const list = Object.values(store.profiles)
      .filter(pr => pr && typeof pr.n === 'string' && isPersistentProfile(pr.n, pr.role))
      .map(pr => ({
        n: pr.n,
        xp: clampXp(pr.xp),
        l: levelOf(pr.xp),
        role: pr.role || ''
      }))
      .filter(x => x.l >= 2 || x.role)
      .sort((a, b) => b.l - a.l || b.xp - a.xp)
      .slice(0, 15);
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(list));
    return;
  }

  if (urlPath === '/sync_profile' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 4096) req.destroy();
    });
    req.on('end', () => {
      try {
        const d = JSON.parse(body || '{}');
        let role = '';
        let name = cleanName(d.n);
        if (d.ap === ADMIN_PW) {
          role = 'admin';
          name = ADMIN_NAME;
        } else if (d.ap === MOD_PW) {
          role = 'mod';
          name = MOD_NAME;
        }
        if (!isPersistentProfile(name, role)) {
          res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
          res.end(JSON.stringify({ ok: false, n: 'JUGADOR', xp: 0, l: 1 }));
          return;
        }
        const merged = mergeProfileXp(name, role, d.xp);
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({
          ok: true,
          n: name,
          xp: merged.xp,
          l: levelOf(merged.xp),
          role: merged.role,
          force: merged.force ? 1 : 0
        }));
      } catch (_) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ ok: false }));
      }
    });
    return;
  }

  if (urlPath === '/leave' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (body.length > 2048) req.destroy();
    });
    req.on('end', () => {
      try {
        const d = JSON.parse(body || '{}');
        for (const [id, p] of players.entries()) {
          if ((d.id && id === d.id) || (d.sid && p.sid === d.sid) || (d.dev && p.dev === d.dev)) {
            removePlayer(id, 'beacon_leave');
          }
        }
      } catch (_) {}
      res.writeHead(204);
      res.end();
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not found', version: VERSION }));
});

// ===== Conexiones WebSocket nativas =====
attachNativeWebSocketServer(server, (ws) => {
  const id = 'P' + (nextPlayerNum++);
  const now = Date.now();
  const p = {
    id,
    ws,
    joined: false,
    n: 'JUGADOR',
    role: '',
    dev: '',
    sid: '',
    x: 0, y: 0, z: 0, yw: 0,
    hp: 100,
    alive: false,
    sl: 0, mv: 0, w: 0, rs: 1,
    pc: null,
    k: 0, d: 0, xp: 0,
    protUntil: 0,
    lastDmgAt: 0,
    lastRegenAt: now,
    lastShotAt: 0,
    lastShotQ: -1,
    burstCredit: 0,
    pendingSpawn: null,
    lastSeen: now,
    hist: []
  };
  players.set(id, p);

  ws.on('pong', () => {
    p.lastSeen = Date.now();
  });

  ws.on('message', (raw) => {
    p.lastSeen = Date.now();
    let m;
    try { m = JSON.parse(String(raw)); } catch (_) { return; }
    if (!m || typeof m !== 'object') return;

    switch (m.t) {
      case 'hba':
        break;

      case 'pg':
        safeSend(ws, { t: 'pn', ts: m.ts });
        break;

      case 'leave':
        removePlayer(id, 'client_leave');
        break;

      case 'hello': {
        sweepDeadPlayers();

        const dev = String(m.dev || '').slice(0, 64);
        const sid = String(m.sid || '').slice(0, 64);
        let role = '';
        let name = cleanName(m.n);

        if (m.ap === ADMIN_PW) {
          role = 'admin';
          name = ADMIN_NAME;
        } else if (m.ap === MOD_PW) {
          role = 'mod';
          name = MOD_NAME;
        }

        const nKey = normKey(name);
        const ban = activeBan(dev, nKey);
        if (ban) {
          safeSend(ws, { t: 'banned', ban: ban.until || 0, r: ban.r || '' });
          setTimeout(() => removePlayer(id, 'banned_join'), 80);
          return;
        }

        if (nKey !== 'jugador') {
          for (const [oid, op] of Array.from(players.entries())) {
            if (oid === id || !op.joined) continue;
            if (normKey(op.n) === nKey) {
              const sameUser = (dev && op.dev && op.dev === dev) || (sid && op.sid && op.sid === sid);
              const isDeadOrStale = (!op.ws || op.ws.readyState !== 1 || (Date.now() - op.lastSeen > STALE_NAME_MS));
              if (sameUser || isDeadOrStale || role) {
                removePlayer(oid, sameUser ? 'reconnect_replace' : 'stale_replace');
              } else {
                safeSend(ws, { t: 'nameError' });
                setTimeout(() => removePlayer(id, 'name_in_use'), 80);
                return;
              }
            }
          }
        }

        const wasJoined = p.joined;
        p.n = name;
        p.role = role;
        p.dev = dev;
        p.sid = sid;
        p.w = (Number.isInteger(m.w) && m.w >= 0 && m.w < WEAPONS.length) ? m.w : 0;

        // Fusionar progreso guardado del jugador/Admin/Mod sin resetear nunca a Nivel 1
        const merged = mergeProfileXp(p.n, p.role, m.xp);
        p.xp = merged.xp;

        const nowJoin = Date.now();
        if (!wasJoined) {
          p.joined = true;
          const sp = pickSpawnPoint(p.id);
          p.x = sp[0]; p.y = 0; p.z = sp[1];
          p.hp = 100;
          p.alive = true;
          p.protUntil = nowJoin + SPAWN_PROT_MS;
          p.lastDmgAt = 0;
          p.lastRegenAt = nowJoin;
          p.hist = [{ t: nowJoin, x: p.x, y: 0, z: p.z, yw: p.yw, sl: 0 }];
          safeSend(ws, { t: 'wel', id: p.id, v: VERSION });
        }

        safeSend(ws, { t: 'level', xp: p.xp, force: merged.force ? 1 : 0 });

        const remMin = Math.max(0.1, (matchEndAt - nowJoin) / 60000);
        const remProt = Math.max(0, p.protUntil - nowJoin);
        safeSend(ws, { t: 'go', map: curMap, lim: MATCH_LIM, min: remMin, sc: buildScoreboard() });
        safeSend(ws, { t: 'sp', x: p.x, y: p.y, z: p.z, hp: p.hp, rs: p.rs, prot: remProt });

        for (const op of players.values()) {
          if (!op.joined || op.id === p.id) continue;
          safeSend(ws, {
            t: 'st', id: op.id,
            x: op.x, y: op.y, z: op.z, yw: op.yw,
            a: op.alive ? 1 : 0, rs: op.rs, w: op.w,
            sl: op.sl, pr: nowJoin < op.protUntil ? 1 : 0, mv: op.mv, pc: op.pc
          });
        }

        broadcast({ t: 'ro', ro: buildRoster() });
        broadcast({ t: 'sc', sc: buildScoreboard() });
        if (!wasJoined && (p.role === 'admin' || p.role === 'mod')) {
          broadcast({ t: 'sysadm', n: p.n }, p.id);
        }
        break;
      }

      case 'st': {
        if (!p.joined) return;
        if (![m.x, m.y, m.z, m.yw].every(Number.isFinite)) return;
        p.x = Math.max(-31, Math.min(31, +m.x));
        p.y = Math.max(-2, Math.min(20, +m.y));
        p.z = Math.max(-31, Math.min(31, +m.z));
        p.yw = +m.yw;
        p.sl = m.sl ? 1 : 0;
        p.mv = (m.mv | 0);
        if (Number.isInteger(m.w) && m.w >= 0 && m.w < WEAPONS.length) p.w = m.w;
        if (Array.isArray(m.pc) && m.pc.length === 3) p.pc = m.pc;

        const nowSt = Date.now();
        p.hist.push({ t: nowSt, x: p.x, y: p.y, z: p.z, yw: p.yw, sl: p.sl });
        if (p.hist.length > 32) p.hist.shift();

        broadcast({
          t: 'st',
          id: p.id,
          x: p.x, y: p.y, z: p.z, yw: p.yw,
          a: p.alive ? 1 : 0,
          rs: p.rs,
          w: p.w,
          sl: p.sl,
          pr: nowSt < p.protUntil ? 1 : 0,
          mv: p.mv,
          pc: p.pc
        }, p.id);
        break;
      }

      case 'shot':
        handleShot(p, m);
        break;

      case 'rq': {
        if (!p.joined || !matchPlaying) return;
        const sp = p.pendingSpawn || pickSpawnPoint(p.id);
        p.pendingSpawn = null;
        const nowRq = Date.now();
        p.x = sp[0]; p.y = 0; p.z = sp[1];
        p.hp = 100;
        p.alive = true;
        p.rs++;
        p.protUntil = nowRq + SPAWN_PROT_MS;
        p.lastDmgAt = 0;
        p.lastRegenAt = nowRq;
        p.hist = [{ t: nowRq, x: p.x, y: 0, z: p.z, yw: p.yw, sl: 0 }];
        safeSend(ws, { t: 'sp', x: p.x, y: p.y, z: p.z, hp: 100, rs: p.rs, prot: SPAWN_PROT_MS });
        broadcast({
          t: 'st', id: p.id,
          x: p.x, y: p.y, z: p.z, yw: p.yw,
          a: 1, rs: p.rs, w: p.w, sl: 0, pr: 1, mv: 0, pc: p.pc
        }, p.id);
        break;
      }

      case 'chat': {
        if (!p.joined) return;
        const text = String(m.m || '').trim().slice(0, 140);
        if (!text) return;
        broadcast({ t: 'chat', id: p.id, n: p.n, m: text }, p.id);
        break;
      }

      case 'vc': {
        if (!p.joined || !m.to) return;
        const target = players.get(String(m.to));
        if (!target || !target.joined) return;
        safeSend(target.ws, {
          t: 'vc',
          from: p.id,
          to: target.id,
          sig: m.sig,
          sdp: m.sdp,
          candidate: m.candidate || m.cand
        });
        break;
      }

      case 'xp': {
        if (!p.joined) return;
        if (!isPersistentProfile(p.n, p.role)) {
          p.xp = 0;
          safeSend(ws, { t: 'level', xp: 0 });
          break;
        }
        const merged = mergeProfileXp(p.n, p.role, m.tot);
        p.xp = merged.xp;
        safeSend(ws, { t: 'level', xp: p.xp, force: merged.force ? 1 : 0 });
        break;
      }

      case 'adm':
        if (p.joined) handleAdminCmd(p, m);
        break;
    }
  });

  ws.on('close', () => removePlayer(id, 'ws_close'));
  ws.on('error', () => removePlayer(id, 'ws_error'));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[GogRivals Server v${VERSION}] Escuchando en 0.0.0.0:${PORT} · Hitbox exacta + SpawnProt 2.5s + Top Persistente · Mapa: ${curMap}`);
});
