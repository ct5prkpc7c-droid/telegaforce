process.on('uncaughtException', e => { console.error('UNCAUGHT:', e); });
process.on('unhandledRejection', e => { console.error('UNHANDLED:', e); });

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || './data.json';

let db = { users: {}, messages: {}, groups: {} };
try { db = { ...db, ...JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) }; } catch (_) {}
if (!db.groups) db.groups = {};
let saveTimer = null;
function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    try { fs.writeFileSync(DATA_FILE, JSON.stringify(db)); } catch (e) { console.error('save error', e); }
    saveTimer = null;
  }, 500);
}

const server = http.createServer((req, res) => {
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };
  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }

  if (req.url === '/' || req.url === '/index.html') {
    try {
      const html = fs.readFileSync(path.join(__dirname, 'index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } catch (e) {
      res.writeHead(404); res.end('index.html not found');
    }
    return;
  }

  if (req.url === '/api/register' && req.method === 'POST') {
    return readJSON(req, body => {
      const { username, password, firstName, lastName, middleName, avatar, display } = body;
      if (!username || !password || !firstName || !lastName) {
        return json(res, 400, { error: 'Missing fields' }, cors);
      }
      if (db.users[username]) return json(res, 409, { error: 'Username taken' }, cors);
      db.users[username] = {
        username, password, firstName, lastName,
        middleName: middleName || '',
        display: display || (firstName + ' ' + lastName),
        avatar: avatar || null,
        createdAt: Date.now(),
        lastSeen: Date.now()
      };
      save();
      broadcastUserlist();
      json(res, 200, { ok: true, user: publicUser(db.users[username]) }, cors);
    });
  }

  if (req.url === '/api/login' && req.method === 'POST') {
    return readJSON(req, body => {
      const { username, password } = body;
      const u = db.users[username];
      if (!u || u.password !== password) return json(res, 401, { error: 'Wrong credentials' }, cors);
      u.lastSeen = Date.now();
      save();
      json(res, 200, { ok: true, user: publicUser(u) }, cors);
    });
  }

  if (req.url === '/api/users' && req.method === 'GET') {
    const list = Object.values(db.users).map(publicUser);
    return json(res, 200, list, cors);
  }

  if (req.url.startsWith('/api/user/') && req.method === 'GET') {
    const uname = decodeURIComponent(req.url.replace('/api/user/', '')).toLowerCase().trim();
    const u = db.users[uname];
    if (!u) return json(res, 404, { error: 'Not found' }, cors);
    return json(res, 200, publicUser(u), cors);
  }

  if (req.url.startsWith('/api/messages/') && req.method === 'GET') {
    const key = decodeURIComponent(req.url.replace('/api/messages/', ''));
    return json(res, 200, db.messages[key] || [], cors);
  }

  if (req.url === '/api/delete-account' && req.method === 'POST') {
    return readJSON(req, body => {
      const { username, password } = body;
      const u = db.users[username];
      if (!u || u.password !== password) return json(res, 401, { error: 'Auth' }, cors);
      delete db.users[username];
      for (const k of Object.keys(db.messages)) {
        if (k.includes(username)) delete db.messages[k];
      }
      save();
      broadcastUserlist();
      json(res, 200, { ok: true }, cors);
    });
  }

  res.writeHead(404, cors); res.end('Not found');
});

function readJSON(req, cb) {
  let data = '';
  let aborted = false;
  req.on('data', c => {
    if (aborted) return;
    data += c;
    if (data.length > 8 * 1024 * 1024) { aborted = true; cb({ _tooLarge: true }); req.destroy(); }
  });
  req.on('end', () => { if (aborted) return; try { cb(JSON.parse(data)); } catch (e) { cb({}); } });
}
function json(res, code, body, cors) {
  res.writeHead(code, { ...cors, 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}
function publicUser(u) {
  if (!u) return null;
  const { password, ...rest } = u;
  return rest;
}

const wss = new WebSocketServer({ server });
const clients = new Map();

wss.on('connection', ws => {
  let username = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }

    if (msg.type === 'auth') {
      const u = db.users[msg.username];
      if (!u || u.password !== msg.password) { ws.send(JSON.stringify({ type: 'auth-fail' })); return; }
      username = msg.username;
      if (!clients.has(username)) clients.set(username, new Set());
      clients.get(username).add(ws);
      u.lastSeen = Date.now();
      save();
      ws.send(JSON.stringify({ type: 'auth-ok' }));
      const list = Object.values(db.users).map(publicUser);
      ws.send(JSON.stringify({ type: 'userlist', users: list }));
      const myMessages = {};
      const favK = '__fav__::' + username;
      if (db.messages[favK]) myMessages[favK] = db.messages[favK];
      for (const k of Object.keys(db.messages)) {
        if (k.includes('::') && k !== favK && !k.startsWith('group::')) {
          const parts = k.split('::');
          if (parts.includes(username)) myMessages[k] = db.messages[k];
        }
      }
      ws.send(JSON.stringify({ type: 'history', messages: myMessages }));
      const myGroups = Object.values(db.groups).filter(g => g.members.includes(username));
      ws.send(JSON.stringify({ type: 'groups', groups: myGroups }));
      const groupMsgs = {};
      for (const g of myGroups) {
        const k = 'group::' + g.id;
        if (db.messages[k]) groupMsgs[k] = db.messages[k];
      }
      ws.send(JSON.stringify({ type: 'group-history', messages: groupMsgs }));
      broadcastPresence(username, true);
      return;
    }

    if (!username) return;

    if (msg.type === 'send') {
      const { to, text, medias, replyTo } = msg;
      if (!to) return;
      const isFav = to === '__fav__';
      const key = isFav ? '__fav__::' + username : [username, to].sort().join('::');
      const newMsg = {
        from: username,
        to: isFav ? username : to,
        text: (text || '').slice(0, 2000),
        ts: Date.now(),
        read: isFav,
        ...(medias && medias.length ? { medias } : {}),
        ...(replyTo ? { replyTo } : {})
      };
      db.messages[key] = db.messages[key] || [];
      db.messages[key].push(newMsg);
      save();
      sendTo(username, { type: 'msg', message: newMsg, key });
      if (!isFav && to !== username) sendTo(to, { type: 'msg', message: newMsg, key });
    }

    if (msg.type === 'edit') {
      const { key, ts, text } = msg;
      const arr = db.messages[key]; if (!arr) return;
      const m = arr.find(x => x.ts === ts && x.from === username);
      if (m) { m.text = text; m.edited = true; save(); broadcastChat(key, { type: 'edit', key, ts, text }); }
    }

    if (msg.type === 'delMsg') {
      const { key, ts } = msg;
      if (!db.messages[key]) return;
      db.messages[key] = db.messages[key].filter(m => !(m.ts === ts && m.from === username));
      save();
      broadcastChat(key, { type: 'delMsg', key, ts });
    }

    if (msg.type === 'react') {
      const { key, ts, emoji } = msg;
      const arr = db.messages[key]; if (!arr) return;
      const m = arr.find(x => x.ts === ts);
      if (!m) return;
      m.reactions = m.reactions || {};
      const had = (m.reactions[emoji] || []).includes(username);
      Object.keys(m.reactions).forEach(e => {
        m.reactions[e] = (m.reactions[e] || []).filter(u => u !== username);
        if (m.reactions[e].length === 0) delete m.reactions[e];
      });
      if (!had) {
        m.reactions[emoji] = m.reactions[emoji] || [];
        m.reactions[emoji].push(username);
      }
      save();
      broadcastChat(key, { type: 'react', key, ts, reactions: m.reactions });
    }

    if (msg.type === 'read') {
      const { key } = msg;
      if (db.messages[key]) {
        db.messages[key].forEach(m => { if (m.to === username) m.read = true; });
        save();
      }
    }

    if (msg.type === 'find') {
      const target = (msg.username || '').toLowerCase().trim();
      const u = db.users[target];
      ws.send(JSON.stringify({ type: 'find-result', username: target, user: u ? publicUser(u) : null }));
    }

    if (msg.type === 'group-create') {
      const id = (msg.id && /^g_[a-z0-9_]+$/.test(msg.id)) ? msg.id : ('g_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 7));
      const name = String(msg.name || 'Group').slice(0, 50);
      const members = Array.from(new Set([username, ...(msg.members || []).slice(0, 200)]));
      db.groups[id] = {
        id, name, members,
        admins: { [username]: { tag: '', perms: { edit: true, kick: true, addAdmin: true, delete: true, editMsgs: true } } },
        owner: username,
        avatar: msg.avatar || null,
        requests: {},
        createdAt: Date.now()
      };
      save();
      const payload = { type: 'group-info', group: db.groups[id] };
      for (const m of members) sendTo(m, payload);
    }

    if (msg.type === 'group-add') {
      const g = db.groups[msg.id]; if (!g) return;
      if (!g.members.includes(username)) return;
      const add = String(msg.user || '').toLowerCase();
      if (!db.users[add] || g.members.includes(add)) return;
      const targetUser = db.users[add];
      const requiresReq = targetUser.privacy && targetUser.privacy.friendsOnly;
      if (requiresReq) {
        g.requests = g.requests || {};
        if (g.requests[add]) return;
        g.requests[add] = { by: username, ts: Date.now() };
        save();
        sendTo(add, { type: 'group-request', group: { id: g.id, name: g.name, avatar: g.avatar }, by: username });
        sendTo(username, { type: 'toast', text: 'Запрос отправлен' });
        return;
      }
      g.members.push(add);
      save();
      const payload = { type: 'group-info', group: g };
      for (const m of g.members) sendTo(m, payload);
    }

    if (msg.type === 'group-req-accept') {
      const g = db.groups[msg.id]; if (!g) return;
      if (!g.requests || !g.requests[username]) return;
      delete g.requests[username];
      if (!g.members.includes(username)) g.members.push(username);
      save();
      const payload = { type: 'group-info', group: g };
      for (const m of g.members) sendTo(m, payload);
    }
    if (msg.type === 'group-req-decline') {
      const g = db.groups[msg.id]; if (!g) return;
      if (g.requests && g.requests[username]) { delete g.requests[username]; save(); }
    }

    if (msg.type === 'group-remove') {
      const g = db.groups[msg.id]; if (!g) return;
      const a = g.admins[username];
      if (!a || !(username === g.owner || (a.perms && a.perms.kick))) return;
      const rem = String(msg.user || '').toLowerCase();
      if (rem === g.owner) return;
      g.members = g.members.filter(x => x !== rem);
      delete g.admins[rem];
      save();
      const payload = { type: 'group-info', group: g };
      for (const m of [...g.members, rem]) sendTo(m, payload);
      sendTo(rem, { type: 'group-kicked', id: g.id });
    }

    if (msg.type === 'group-leave') {
      const g = db.groups[msg.id]; if (!g) return;
      if (username === g.owner) return;
      g.members = g.members.filter(x => x !== username);
      delete g.admins[username];
      save();
      const payload = { type: 'group-info', group: g };
      for (const m of g.members) sendTo(m, payload);
      sendTo(username, { type: 'group-kicked', id: g.id });
    }

    if (msg.type === 'group-admin') {
      const g = db.groups[msg.id]; if (!g) return;
      const a = g.admins[username];
      const canAdmin = username === g.owner || (a && a.perms && a.perms.addAdmin);
      if (!canAdmin) return;
      const target = String(msg.user || '').toLowerCase();
      if (!g.members.includes(target)) return;
      if (msg.add) {
        const perms = msg.perms || { edit: false, kick: false, addAdmin: false, delete: false, editMsgs: false };
        g.admins[target] = { tag: String(msg.tag || '').slice(0, 10), perms };
      } else if (target !== g.owner) delete g.admins[target];
      save();
      const payload = { type: 'group-info', group: g };
      for (const m of g.members) sendTo(m, payload);
    }

    if (msg.type === 'group-edit') {
      const g = db.groups[msg.id]; if (!g) return;
      const a = g.admins[username];
      if (!(username === g.owner || (a && a.perms && a.perms.edit))) return;
      if (typeof msg.name === 'string' && msg.name.trim()) g.name = msg.name.slice(0, 50);
      if (typeof msg.avatar !== 'undefined') g.avatar = msg.avatar || null;
      save();
      const payload = { type: 'group-info', group: g };
      for (const m of g.members) sendTo(m, payload);
    }

    if (msg.type === 'group-delete') {
      const g = db.groups[msg.id]; if (!g) return;
      const a = g.admins[username];
      if (!(username === g.owner || (a && a.perms && a.perms.delete))) return;
      const mems = [...g.members];
      delete db.groups[g.id];
      delete db.messages['group::' + g.id];
      save();
      for (const m of mems) sendTo(m, { type: 'group-kicked', id: g.id });
    }

    if (msg.type === 'group-clear') {
      const g = db.groups[msg.id]; if (!g) return;
      const a = g.admins[username];
      if (!(username === g.owner || a)) return;
      db.messages['group::' + g.id] = [];
      save();
      const payload = { type: 'group-clear', id: g.id };
      for (const m of g.members) sendTo(m, payload);
    }

    if (msg.type === 'group-send') {
      const g = db.groups[msg.id]; if (!g) return;
      if (!g.members.includes(username)) return;
      const key = 'group::' + g.id;
      const newMsg = {
        from: username,
        to: g.id,
        text: (msg.text || '').slice(0, 2000),
        ts: Date.now(),
        read: false,
        ...(msg.medias && msg.medias.length ? { medias: msg.medias } : {}),
        ...(msg.replyTo ? { replyTo: msg.replyTo } : {})
      };
      db.messages[key] = db.messages[key] || [];
      db.messages[key].push(newMsg);
      save();
      const payload = { type: 'group-msg', groupId: g.id, message: newMsg };
      for (const m of g.members) sendTo(m, payload);
    }

    if (msg.type === 'ping') {
      const u = db.users[username]; if (u) { u.lastSeen = Date.now(); }
    }
  });

  ws.on('close', () => {
    if (!username) return;
    const set = clients.get(username);
    if (set) { set.delete(ws); if (set.size === 0) clients.delete(username); }
    broadcastPresence(username, false);
  });
});

function sendTo(uname, payload) {
  const set = clients.get(uname);
  if (!set) return;
  const data = JSON.stringify(payload);
  for (const ws of set) { try { ws.send(data); } catch (e) {} }
}
function broadcastChat(key, payload) {
  const parts = key.split('::');
  for (const p of parts) if (p && p !== '__fav__') sendTo(p, payload);
}
function broadcastPresence(uname, online) {
  const data = JSON.stringify({ type: 'presence', username: uname, online, ts: Date.now() });
  for (const set of clients.values()) for (const ws of set) { try { ws.send(data); } catch (e) {} }
}
function broadcastUserlist() {
  const list = Object.values(db.users).map(publicUser);
  const data = JSON.stringify({ type: 'userlist', users: list });
  for (const set of clients.values()) for (const ws of set) { try { ws.send(data); } catch (e) {} }
}

server.listen(PORT, () => {
  console.log('Telegaforce server running on port ' + PORT);
});
