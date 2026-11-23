'use strict';

/**
 * Triangle Drift - online co-op server
 * Supports one host + up to seven guests (8 players total).
 * The host remains authoritative for the shared game state. This server only
 * manages rooms and relays validated messages between guests and the host.
 */

const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const PORT = parsePort(process.env.PORT, 3000);
const MAX_PLAYERS = 8;
const ROOM_CODE_LENGTH = 6;
const ROOM_CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const PUBLIC_DIR = path.resolve(process.env.PUBLIC_DIR || path.join(__dirname, 'public'));
const ALLOWED_ORIGIN = process.env.CORS_ORIGIN || '*';

function parsePort(value, fallback) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 && n <= 65535 ? n : fallback;
}

const app = express();
const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: ALLOWED_ORIGIN === '*' ? '*' : ALLOWED_ORIGIN.split(',').map(s => s.trim()),
    methods: ['GET', 'POST'],
  },
  maxHttpBufferSize: 1e6,
  pingInterval: 25000,
  pingTimeout: 20000,
  perMessageDeflate: { threshold: 1024 },
});

app.disable('x-powered-by');
app.get('/healthz', (_req, res) => res.status(200).json({
  ok: true,
  service: 'triangle-drift',
  maxPlayers: MAX_PLAYERS,
  activeRooms: roomHosts.size,
}));
app.use(express.static(PUBLIC_DIR, { extensions: ['html'], index: 'index.html' }));

// room code -> host socket id. Socket.IO itself maintains room membership.
const roomHosts = new Map();
const roomGames = new Map();

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function finite(value, min = -1e7, max = 1e7) {
  return typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max;
}
function makeRoomCode() {
  let code;
  do {
    code = '';
    for (let i = 0; i < ROOM_CODE_LENGTH; i++) {
      code += ROOM_CODE_CHARS[Math.floor(Math.random() * ROOM_CODE_CHARS.length)];
    }
  } while (io.sockets.adapter.rooms.has(code));
  return code;
}
function getRoomPlayers(code) {
  const members = io.sockets.adapter.rooms.get(code);
  if (!members) return [];
  return [...members]
    .map(socketId => io.sockets.sockets.get(socketId))
    .filter(Boolean)
    .map(s => ({ playerId: s.data.playerId, isHost: s.data.isHost === true, name: s.data.playerName || `P${s.data.playerId}`, color: s.data.playerColor || '', icon: s.data.playerIcon || '' }))
    .filter(p => Number.isInteger(p.playerId))
    .sort((a, b) => a.playerId - b.playerId);
}
function publishRoster(code) {
  if (!code || !roomHosts.has(code)) return;
  io.to(code).emit('room-roster', {
    players: getRoomPlayers(code),
    maxPlayers: MAX_PLAYERS,
  });
}
function roomError(socket, message) {
  socket.emit('room-error', { message });
}
function getHostSocket(code) {
  const id = roomHosts.get(code);
  return id ? io.sockets.sockets.get(id) : undefined;
}
function hasRoomMembership(socket) {
  return typeof socket.data.roomCode === 'string' && Number.isInteger(socket.data.playerId);
}

// Lightweight per-socket event rate limit. It protects the relay without
// interfering with the game's normal input/snapshot cadence.
function allowRate(socket, eventName, limit, intervalMs = 1000) {
  const now = Date.now();
  if (!socket.data.rateLimits) socket.data.rateLimits = new Map();
  const current = socket.data.rateLimits.get(eventName) || { start: now, count: 0 };
  if (now - current.start >= intervalMs) {
    current.start = now;
    current.count = 0;
  }
  current.count += 1;
  socket.data.rateLimits.set(eventName, current);
  return current.count <= limit;
}

function sendGuestEventToHost(socket, eventName, payload) {
  if (!hasRoomMembership(socket) || socket.data.isHost) return;
  const code = socket.data.roomCode;
  const host = getHostSocket(code);
  if (!host) return;
  // Never trust a playerId supplied by the browser.
  const data = isPlainObject(payload)
    ? { ...payload, playerId: socket.data.playerId }
    : { value: payload, playerId: socket.data.playerId };
  host.emit(eventName, data);
}

function validateInput(d) {
  return isPlainObject(d) &&
    finite(d.x, -10000, 10000) && finite(d.y, -10000, 10000) &&
    finite(d.aimX, -10000, 10000) && finite(d.aimY, -10000, 10000) &&
    finite(d.cursorX, -10000, 10000) && finite(d.cursorY, -10000, 10000) &&
    finite(d.dashT, 0, 10) && finite(d.dashCd, 0, 30) &&
    typeof d.firing === 'boolean';
}
function validateBulletHit(d) {
  return isPlainObject(d) && typeof d.bulletId === 'string' && d.bulletId.length <= 100 &&
    Number.isSafeInteger(d.enemyId) && finite(d.damage, 0.001, 1000) &&
    finite(d.kb, 0, 100) && finite(d.vx, -10000, 10000) &&
    finite(d.vy, -10000, 10000) && finite(d.x, -10000, 10000) &&
    finite(d.y, -10000, 10000) && (d.r === undefined || finite(d.r, 0, 100));
}
function validateEnemyBulletHit(d) {
  return isPlainObject(d) && Number.isSafeInteger(d.bulletId) &&
    finite(d.x, -10000, 10000) && finite(d.y, -10000, 10000) &&
    finite(d.fromX, -10000, 10000) && finite(d.fromY, -10000, 10000);
}
function validateEnemyContact(d) {
  return isPlainObject(d) && Number.isSafeInteger(d.enemyId) &&
    finite(d.x, -10000, 10000) && finite(d.y, -10000, 10000) &&
    typeof d.dash === 'boolean';
}
function validateEmoji(d) {
  return isPlainObject(d) && typeof d.emoji === 'string' && d.emoji.length > 0 &&
    d.emoji.length <= 16 && Array.from(d.emoji).length <= 8 && !/[\u0000-\u001f\u007f]/.test(d.emoji);
}

io.on('connection', socket => {
  socket.data.roomCode = null;
  socket.data.playerId = null;
  socket.data.isHost = false;
  socket.data.playerName = '';
  socket.data.playerIcon = '';

  socket.on('set-name', payload => {
    if (!hasRoomMembership(socket) || !isPlainObject(payload)) return;
    const name = typeof payload.name === 'string' ? payload.name.trim().slice(0, 16) : '';
    socket.data.playerName = name || `P${socket.data.playerId}`;
    publishRoster(socket.data.roomCode);
  });
  socket.on('set-color', payload => {
    if (!hasRoomMembership(socket) || !isPlainObject(payload) || typeof payload.color !== 'string' || !/^#[0-9a-f]{6}$/i.test(payload.color)) return;
    socket.data.playerColor = payload.color;
    publishRoster(socket.data.roomCode);
  });
  socket.on('set-icon', payload => {
    if (!hasRoomMembership(socket) || !isPlainObject(payload) || typeof payload.icon !== 'string') return;
    const icon = payload.icon === '' ? '' : payload.icon;
    if (icon && (!/^data:image\/(png|jpeg|webp);base64,/i.test(icon) || icon.length > 220000)) return;
    socket.data.playerIcon = icon;
    publishRoster(socket.data.roomCode);
  });

  socket.on('create-room', () => {
    if (hasRoomMembership(socket)) {
      roomError(socket, 'すでにルームに参加しています。');
      return;
    }
    const code = makeRoomCode();
    socket.join(code);
    socket.data.roomCode = code;
    socket.data.playerId = 1;
    socket.data.isHost = true;
    socket.data.playerName = `P${socket.data.playerId}`;
    roomHosts.set(code, socket.id);
    roomGames.set(code, false);
    socket.emit('room-created', { code, playerId: 1, maxPlayers: MAX_PLAYERS });
    publishRoster(code);
  });

  socket.on('join-room', data => {
    if (hasRoomMembership(socket)) {
      roomError(socket, 'すでにルームに参加しています。');
      return;
    }
    const code = String(data && data.code ? data.code : '').trim().toUpperCase();
    const room = io.sockets.adapter.rooms.get(code);
    const host = getHostSocket(code);
    if (!/^[A-HJ-NP-Z2-9]{6}$/.test(code) || !room || !host || host.data.isHost !== true) {
      roomError(socket, 'ルームが見つかりません。コードを確認してください。');
      return;
    }
    if (room.size >= MAX_PLAYERS) {
      roomError(socket, `このルームは満員です（最大${MAX_PLAYERS}人）。`);
      return;
    }

    const usedIds = new Set(getRoomPlayers(code).map(p => p.playerId));
    let playerId = 2;
    while (usedIds.has(playerId) && playerId <= MAX_PLAYERS) playerId++;
    if (playerId > MAX_PLAYERS) {
      roomError(socket, `このルームは満員です（最大${MAX_PLAYERS}人）。`);
      return;
    }

    socket.join(code);
    socket.data.roomCode = code;
    socket.data.playerId = playerId;
    socket.data.isHost = false;
    socket.data.playerName = `P${playerId}`;
    socket.emit('room-joined', { code, playerId, maxPlayers: MAX_PLAYERS, inGame: roomGames.get(code) === true });
    socket.to(code).emit('peer-joined', { playerId, name: socket.data.playerName });
    publishRoster(code);
  });

  // Host -> guests: authoritative game start and world snapshots.
  socket.on('start-game', () => {
    if (!hasRoomMembership(socket) || !socket.data.isHost || !allowRate(socket, 'start-game', 3)) return;
    roomGames.set(socket.data.roomCode, true);
    socket.to(socket.data.roomCode).emit('start-game');
  });
  socket.on('cpu-count', payload => {
    if (!hasRoomMembership(socket) || !socket.data.isHost || !isPlainObject(payload) ||
        !Number.isInteger(payload.count) || payload.count < 0 ||
        payload.count > MAX_PLAYERS - getRoomPlayers(socket.data.roomCode).length ||
        !allowRate(socket, 'cpu-count', 12)) return;
    socket.to(socket.data.roomCode).emit('cpu-count', { count: payload.count });
  });
  socket.on('end-game', () => {
    if (!hasRoomMembership(socket) || !socket.data.isHost || !allowRate(socket, 'end-game', 3)) return;
    roomGames.set(socket.data.roomCode, false);
    socket.to(socket.data.roomCode).emit('game-ended');
  });
  socket.on('enter-game', () => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !allowRate(socket, 'enter-game', 5)) return;
    sendGuestEventToHost(socket, 'enter-game', {});
  });
  socket.on('return-lobby', () => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !allowRate(socket, 'return-lobby', 5)) return;
    sendGuestEventToHost(socket, 'guest-returned-lobby', {});
  });
  socket.on('snapshot', payload => {
    if (!hasRoomMembership(socket) || !socket.data.isHost || !isPlainObject(payload) ||
        !allowRate(socket, 'snapshot', 45)) return;
    socket.to(socket.data.roomCode).volatile.emit('snapshot', payload);
  });

  // Guest -> host: input, upgrade choice and locally computed hit reports.
  socket.on('input', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !validateInput(payload) || !allowRate(socket, 'input', 80)) return;
    sendGuestEventToHost(socket, 'input', payload);
  });
  socket.on('player-emoji', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !validateEmoji(payload) ||
        !allowRate(socket, 'player-emoji', 8)) return;
    sendGuestEventToHost(socket, 'player-emoji', { emoji: payload.emoji });
  });
  socket.on('upgrade-pick', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !isPlainObject(payload) ||
        !Number.isInteger(payload.index) || payload.index < 0 || payload.index > 100 ||
        !allowRate(socket, 'upgrade-pick', 5)) return;
    sendGuestEventToHost(socket, 'upgrade-pick', { index: payload.index });
  });
  socket.on('upgrade-reroll', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !isPlainObject(payload) ||
        !allowRate(socket, 'upgrade-reroll', 5)) return;
    sendGuestEventToHost(socket, 'upgrade-reroll', {});
  });
  socket.on('late-upgrade-pick', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !isPlainObject(payload) ||
        !Number.isInteger(payload.index) || payload.index < 0 || payload.index > 2 ||
        !allowRate(socket, 'late-upgrade-pick', 5)) return;
    sendGuestEventToHost(socket, 'late-upgrade-pick', { index: payload.index });
  });
  socket.on('job-pick', payload => {
    const jobs = ['gunner', 'trick', 'assassin', 'guardian', 'paladin'];
    if (!hasRoomMembership(socket) || socket.data.isHost || !isPlainObject(payload) ||
        !jobs.includes(payload.jobId) || !allowRate(socket, 'job-pick', 3)) return;
    sendGuestEventToHost(socket, 'job-pick', { jobId: payload.jobId });
  });
  socket.on('bullet-hit', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !validateBulletHit(payload) || !allowRate(socket, 'bullet-hit', 180)) return;
    sendGuestEventToHost(socket, 'bullet-hit', payload);
  });
  socket.on('enemy-bullet-hit', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !validateEnemyBulletHit(payload) || !allowRate(socket, 'enemy-bullet-hit', 40)) return;
    sendGuestEventToHost(socket, 'enemy-bullet-hit', payload);
  });
  socket.on('enemy-contact', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !validateEnemyContact(payload) || !allowRate(socket, 'enemy-contact', 80)) return;
    sendGuestEventToHost(socket, 'enemy-contact', payload);
  });
  // Kept for compatibility with client versions that may send player-state.
  socket.on('player-state', payload => {
    if (!hasRoomMembership(socket) || socket.data.isHost || !isPlainObject(payload) || !allowRate(socket, 'player-state', 40)) return;
    sendGuestEventToHost(socket, 'player-state', payload);
  });

  // Host -> guests: optional state/effect events used by compatible clients.
  for (const eventName of ['damage-result', 'enemy-state', 'enemy-killed', 'revive']) {
    socket.on(eventName, payload => {
      if (!hasRoomMembership(socket) || !socket.data.isHost || !allowRate(socket, eventName, 60)) return;
      socket.to(socket.data.roomCode).emit(eventName, payload);
    });
  }

  socket.on('disconnect', () => {
    const code = socket.data.roomCode;
    if (!code) return;

    if (socket.data.isHost) {
      // A room cannot continue without its authoritative host.
      roomHosts.delete(code);
      roomGames.delete(code);
      socket.to(code).emit('peer-left', { hostLeft: true, playerId: 1 });
      for (const socketId of io.sockets.adapter.rooms.get(code) || []) {
        const member = io.sockets.sockets.get(socketId);
        if (member) {
          member.data.roomCode = null;
          member.data.playerId = null;
          member.data.isHost = false;
          member.leave(code);
          roomError(member, 'ホストが退出したため、ルームは終了しました。');
        }
      }
    } else {
      socket.to(code).emit('peer-left', { playerId: socket.data.playerId, name: socket.data.playerName || `P${socket.data.playerId}` });
      // Socket.IO removes the disconnecting socket from rooms after this handler.
      setImmediate(() => {
        if (roomHosts.has(code)) publishRoster(code);
      });
    }
  });
});

httpServer.listen(PORT, () => {
  console.log(`Triangle Drift server listening on port ${PORT}`);
  console.log(`Static files: ${PUBLIC_DIR}`);
});

function shutdown(signal) {
  console.log(`${signal} received; shutting down...`);
  io.close(() => {
    httpServer.close(() => process.exit(0));
  });
  setTimeout(() => process.exit(1), 10000).unref();
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
