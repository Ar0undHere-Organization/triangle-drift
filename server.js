const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// file:// からの直接アクセスや別ドメインからの接続を許可
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  // スナップショットは同じキーの繰り返しが多いJSONなので、一定サイズ以上は圧縮して帯域を節約
  perMessageDeflate: { threshold: 1024 }
});

// publicフォルダ内のHTMLファイルを配信する設定（URL入力の手間を省く用）
app.use(express.static(path.join(__dirname, 'public')));

// 6文字のランダムなルームコード生成
function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 見間違いやすい文字を除外
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}

io.on('connection', (socket) => {
  // 1. ルーム作成（ホスト）
  socket.on('create-room', () => {
    let code = generateCode();
    // 万が一コードが被ったら再生成
    while (io.sockets.adapter.rooms.has(code)) {
      code = generateCode();
    }

    socket.join(code);
    socket.data.roomCode = code;
    socket.data.isHost = true;

    // クライアントの d.code に合わせてオブジェクトで返す
    socket.emit('room-created', { code });
  });

  // 2. ルーム参加（ゲスト）
  socket.on('join-room', (data) => {
    // クライアントからは { code } で送られてくる
    const code = (data && data.code ? String(data.code) : '').trim().toUpperCase();
    const room = io.sockets.adapter.rooms.get(code);

    if (!code || !room || room.size === 0) {
      return socket.emit('room-error', { message: 'ルームが見つかりません。コードを確認してください。' });
    }
    if (room.size >= 2) {
      return socket.emit('room-error', { message: 'このルームは満員です。' });
    }

    socket.join(code);
    socket.data.roomCode = code;
    socket.data.isHost = false;

    // クライアントの d.code に合わせてオブジェクトで返す
    socket.emit('room-joined', { code });
    // ホストへ参加を通知
    socket.to(code).emit('peer-joined');
  });

  // 3. ゲーム中のイベント中継（ルーム内のもう1人へそのまま転送）
  //    ・snapshot : ホスト→ゲスト。演出イベントも含むので確実に届ける(通常のemit)
  //    ・input    : ゲスト→ホスト。位置・射撃状態を毎回丸ごと送る方式なので、
  //                 回線が詰まった時は古いものを捨てて最新だけ届ける(volatile)
  //    ・start-game / upgrade-pick : 1回きりのイベントなので確実に届ける
  const relayEvents = ['start-game', 'snapshot', 'upgrade-pick'];
  relayEvents.forEach((eventName) => {
    socket.on(eventName, (payload) => {
      if (socket.data.roomCode) {
        socket.to(socket.data.roomCode).emit(eventName, payload);
      }
    });
  });
  socket.on('input', (payload) => {
    if (socket.data.roomCode) {
      socket.to(socket.data.roomCode).volatile.emit('input', payload);
    }
  });

  // 4. 切断時の通知
  socket.on('disconnect', () => {
    if (socket.data.roomCode) {
      socket.to(socket.data.roomCode).emit('peer-left');
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});