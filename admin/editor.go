package admin

const editorHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GoMQ – Topology Editor</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0a0e17;--bg2:#111827;--bg-card:#1a2236;--bg-hover:#1f2942;
  --border:#2a3550;--text:#e2e8f0;--text2:#94a3b8;--muted:#64748b;
  --blue:#3b82f6;--purple:#8b5cf6;--cyan:#06b6d4;--emerald:#10b981;
  --amber:#f59e0b;--rose:#f43f5e;
  --grad-blue:linear-gradient(135deg,#3b82f6,#2563eb);
  --grad-purple:linear-gradient(135deg,#8b5cf6,#7c3aed);
  --grad-emerald:linear-gradient(135deg,#10b981,#059669);
  --grad-amber:linear-gradient(135deg,#f59e0b,#d97706);
  --shadow:0 4px 24px rgba(0,0,0,.35);
  --radius:14px;
}
html,body{height:100%;overflow:hidden;font-family:'Inter',sans-serif;background:var(--bg);color:var(--text)}
body::before{content:'';position:fixed;inset:0;background:radial-gradient(ellipse 800px 600px at 20% 10%,rgba(59,130,246,.06),transparent),radial-gradient(ellipse 600px 500px at 80% 80%,rgba(139,92,246,.05),transparent);pointer-events:none;z-index:0}

/* ── NAV ── */
.nav{position:fixed;top:0;left:0;right:0;z-index:100;backdrop-filter:blur(20px) saturate(180%);background:rgba(10,14,23,.8);border-bottom:1px solid var(--border);padding:0 24px;height:56px;display:flex;align-items:center;justify-content:space-between}
.nav-brand{display:flex;align-items:center;gap:10px;font-weight:700;font-size:1rem}
.nav-brand .logo{width:32px;height:32px;border-radius:8px;background:var(--grad-blue);display:grid;place-items:center;font-size:.85rem;font-weight:800;color:#fff;box-shadow:0 0 16px rgba(59,130,246,.3)}
.nav-links{display:flex;gap:6px}
.nav-link{font-family:inherit;font-size:.8rem;font-weight:500;padding:6px 16px;border:1px solid var(--border);border-radius:8px;background:transparent;color:var(--text2);cursor:pointer;text-decoration:none;transition:.2s}
.nav-link:hover{background:var(--bg-card);color:var(--text);border-color:var(--blue)}
.nav-link.primary{background:var(--grad-emerald);border-color:transparent;color:#fff}
.nav-link.primary:hover{box-shadow:0 4px 16px rgba(16,185,129,.35);transform:translateY(-1px)}

/* ── SIDEBAR ── */
.sidebar{position:fixed;top:56px;left:0;bottom:0;width:260px;z-index:50;background:var(--bg-card);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow-y:auto}
.sidebar-section{padding:16px 18px;border-bottom:1px solid var(--border)}
.sidebar-title{font-size:.7rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:12px}
.add-btn{width:100%;padding:10px 14px;border:1px dashed var(--border);border-radius:10px;background:transparent;color:var(--text2);font-family:inherit;font-size:.82rem;font-weight:500;cursor:pointer;display:flex;align-items:center;gap:10px;transition:.2s;margin-bottom:8px}
.add-btn:hover{background:var(--bg-hover);border-color:var(--blue);color:var(--text)}
.add-btn .icon{width:32px;height:32px;border-radius:8px;display:grid;place-items:center;font-size:.9rem;flex-shrink:0}
.add-btn .icon.ex{background:rgba(139,92,246,.15);color:var(--purple)}
.add-btn .icon.qu{background:rgba(59,130,246,.15);color:var(--blue)}

/* Property panel */
.prop-panel{display:none;padding:16px 18px}
.prop-panel.visible{display:block}
.prop-title{font-size:.82rem;font-weight:600;margin-bottom:14px;display:flex;align-items:center;gap:8px}
.prop-group{margin-bottom:12px}
.prop-label{font-size:.7rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px;display:block}
.prop-input{width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:8px;background:rgba(0,0,0,.2);color:var(--text);font-family:'JetBrains Mono',monospace;font-size:.82rem;outline:none;transition:border-color .2s}
.prop-input:focus{border-color:var(--blue)}
.prop-select{width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:8px;background:rgba(0,0,0,.2);color:var(--text);font-family:inherit;font-size:.82rem;outline:none;appearance:none;cursor:pointer}
.prop-select option{background:var(--bg-card);color:var(--text)}
.btn-delete{width:100%;padding:8px;border:1px solid rgba(244,63,94,.3);border-radius:8px;background:rgba(244,63,94,.08);color:var(--rose);font-family:inherit;font-size:.78rem;font-weight:600;cursor:pointer;transition:.2s;margin-top:8px}
.btn-delete:hover{background:rgba(244,63,94,.18);border-color:var(--rose)}

/* ── CANVAS ── */
.canvas-wrap{position:fixed;top:56px;left:260px;right:0;bottom:0;overflow:hidden;z-index:1}
.canvas{position:relative;width:4000px;height:4000px}
.canvas-grid{position:absolute;inset:0;background-image:radial-gradient(circle,rgba(255,255,255,.04) 1px,transparent 1px);background-size:28px 28px;pointer-events:none}
svg.connections{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;z-index:1}
svg.connections path{fill:none;stroke:var(--blue);stroke-width:2;opacity:.6;transition:opacity .2s}
svg.connections path:hover{opacity:1;stroke-width:3}
svg.connections text{fill:var(--text2);font-family:'JetBrains Mono',monospace;font-size:11px}

/* ── NODE ── */
.node{position:absolute;min-width:180px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg-card);box-shadow:var(--shadow);cursor:grab;user-select:none;z-index:10;transition:box-shadow .2s,border-color .2s}
.node:active{cursor:grabbing}
.node.selected{border-color:var(--blue);box-shadow:0 0 0 2px rgba(59,130,246,.25),var(--shadow)}
.node-header{padding:10px 14px;border-radius:var(--radius) var(--radius) 0 0;font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.8px;display:flex;align-items:center;justify-content:space-between}
.node-header.exchange{background:rgba(139,92,246,.12);color:var(--purple)}
.node-header.queue{background:rgba(59,130,246,.12);color:var(--blue)}
.node-body{padding:10px 14px}
.node-name{font-family:'JetBrains Mono',monospace;font-size:.85rem;color:var(--cyan);word-break:break-all}
.node-meta{font-size:.7rem;color:var(--muted);margin-top:4px}
.node-tag{display:inline-block;padding:1px 8px;border-radius:4px;font-size:.65rem;font-weight:600;text-transform:uppercase}
.node-tag.direct{background:rgba(59,130,246,.12);color:var(--blue)}
.node-tag.fanout{background:rgba(139,92,246,.12);color:var(--purple)}
.node-tag.topic{background:rgba(245,158,11,.12);color:var(--amber)}

/* Ports */
.port{position:absolute;width:14px;height:14px;border-radius:50%;background:var(--bg);border:2px solid var(--border);cursor:crosshair;z-index:20;transition:background .15s,border-color .15s,transform .15s}
.port:hover{border-color:var(--blue);background:rgba(59,130,246,.2);transform:scale(1.3)}
.port.out{right:-7px;top:50%}
.port.in{left:-7px;top:50%}
.port.active{background:var(--blue);border-color:var(--blue);animation:portPulse 1s ease infinite}
@keyframes portPulse{0%,100%{box-shadow:0 0 0 0 rgba(59,130,246,.4)}50%{box-shadow:0 0 0 6px rgba(59,130,246,0)}}

/* Connection routing key label input */
.conn-label-input{position:absolute;z-index:30;padding:4px 10px;border:1px solid var(--blue);border-radius:6px;background:var(--bg-card);color:var(--cyan);font-family:'JetBrains Mono',monospace;font-size:.75rem;outline:none;min-width:80px;box-shadow:0 4px 16px rgba(0,0,0,.4)}

/* ── Toast ── */
.toast{position:fixed;bottom:24px;right:24px;z-index:500;padding:12px 24px;border-radius:10px;font-size:.85rem;font-weight:500;color:#fff;box-shadow:0 6px 24px rgba(0,0,0,.4);animation:toastIn .3s ease;pointer-events:none}
.toast-ok{background:var(--grad-emerald)}
.toast-err{background:linear-gradient(135deg,#f43f5e,#e11d48)}
@keyframes toastIn{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}

/* ── Existing badge ── */
.badge-existing{font-size:.6rem;padding:1px 6px;border-radius:4px;background:rgba(16,185,129,.12);color:var(--emerald);font-weight:600;margin-left:6px;text-transform:uppercase;letter-spacing:.5px}
</style>
</head>
<body>

<!-- NAV -->
<nav class="nav">
  <div class="nav-brand">
    <div class="logo">MQ</div>
    <span>Topology Editor</span>
  </div>
  <div class="nav-links">
    <a class="nav-link" href="/">← Dashboard</a>
    <button class="nav-link" onclick="loadTopology()" title="Import current broker state">⟳ Load Topology</button>
    <button class="nav-link" onclick="clearCanvas()">✕ Clear</button>
    <button class="nav-link primary" onclick="deploy()">🚀 Deploy</button>
  </div>
</nav>

<!-- SIDEBAR -->
<div class="sidebar">
  <div class="sidebar-section">
    <div class="sidebar-title">Add Nodes</div>
    <button class="add-btn" onclick="addExchangeNode()">
      <div class="icon ex">🔀</div>
      <div><div style="font-weight:600;color:var(--text)">Exchange</div><div style="font-size:.72rem;color:var(--muted)">Route messages</div></div>
    </button>
    <button class="add-btn" onclick="addQueueNode()">
      <div class="icon qu">📦</div>
      <div><div style="font-weight:600;color:var(--text)">Queue</div><div style="font-size:.72rem;color:var(--muted)">Store messages</div></div>
    </button>
  </div>
  <div class="sidebar-section" style="flex:1">
    <div class="sidebar-title">Instructions</div>
    <div style="font-size:.78rem;color:var(--text2);line-height:1.6">
      <p style="margin-bottom:8px">1. Add exchanges and queues using the buttons above.</p>
      <p style="margin-bottom:8px">2. Drag nodes to arrange them.</p>
      <p style="margin-bottom:8px">3. Click an exchange's <span style="color:var(--blue)">output port</span> (right dot), then click a queue's <span style="color:var(--blue)">input port</span> (left dot) to create a binding.</p>
      <p style="margin-bottom:8px">4. Click a node to edit its properties in this panel.</p>
      <p style="margin-bottom:8px">5. Click <strong style="color:var(--emerald)">Deploy</strong> to create everything on the broker.</p>
    </div>
  </div>
  <div class="prop-panel" id="prop-panel">
    <div class="sidebar-title">Properties</div>
    <div id="prop-content"></div>
  </div>
</div>

<!-- CANVAS -->
<div class="canvas-wrap" id="canvas-wrap">
  <div class="canvas" id="canvas">
    <div class="canvas-grid"></div>
    <svg class="connections" id="svg-connections"></svg>
  </div>
</div>

<script>
// ─── State ──────────────────────────────────────────────────
let nodes = [];       // {id, type:'exchange'|'queue', name, x, y, subtype?, size?, existing?}
let connections = []; // {id, fromId, toId, routingKey}
let nextId = 1;
let selectedId = null;
let connectingFrom = null; // node id we're drawing a connection from
let dragState = null;      // {nodeId, offsetX, offsetY}

const canvas = document.getElementById('canvas');
const wrap = document.getElementById('canvas-wrap');
const svgEl = document.getElementById('svg-connections');

// ─── Node creation ──────────────────────────────────────────
function addExchangeNode(name, subtype, x, y, existing) {
  const id = nextId++;
  const node = {
    id, type:'exchange',
    name: name || 'new_exchange',
    subtype: subtype || 'direct',
    x: x ?? 200 + Math.random()*200,
    y: y ?? 100 + nodes.filter(n=>n.type==='exchange').length * 140,
    existing: existing || false
  };
  nodes.push(node);
  renderNode(node);
  renderConnections();
  return node;
}

function addQueueNode(name, size, x, y, existing) {
  const id = nextId++;
  const node = {
    id, type:'queue',
    name: name || 'new_queue',
    size: size || 1000,
    x: x ?? 650 + Math.random()*200,
    y: y ?? 100 + nodes.filter(n=>n.type==='queue').length * 140,
    existing: existing || false
  };
  nodes.push(node);
  renderNode(node);
  renderConnections();
  return node;
}

function addConnection(fromId, toId, routingKey) {
  // Avoid duplicates
  if (connections.some(c => c.fromId === fromId && c.toId === toId && c.routingKey === routingKey)) return;
  const id = nextId++;
  connections.push({id, fromId, toId, routingKey: routingKey || '', existing: false});
  renderConnections();
}

// ─── Render node DOM ────────────────────────────────────────
function renderNode(n) {
  const el = document.createElement('div');
  el.className = 'node';
  el.id = 'node-' + n.id;
  el.dataset.nodeId = n.id;
  el.style.left = n.x + 'px';
  el.style.top = n.y + 'px';
  updateNodeContent(el, n);
  // Drag
  el.addEventListener('mousedown', e => {
    if (e.target.classList.contains('port')) return;
    e.preventDefault();
    selectNode(n.id);
    const rect = el.getBoundingClientRect();
    const wrapRect = wrap.getBoundingClientRect();
    dragState = {
      nodeId: n.id,
      offsetX: e.clientX - rect.left,
      offsetY: e.clientY - rect.top,
      scrollX: wrap.scrollLeft,
      scrollY: wrap.scrollTop,
      wrapLeft: wrapRect.left,
      wrapTop: wrapRect.top
    };
  });
  // Select on click
  el.addEventListener('click', e => {
    if (!e.target.classList.contains('port')) selectNode(n.id);
  });
  canvas.appendChild(el);
}

function updateNodeContent(el, n) {
  const isExchange = n.type === 'exchange';
  const headerClass = isExchange ? 'exchange' : 'queue';
  const label = isExchange ? 'Exchange' : 'Queue';
  const existBadge = n.existing ? '<span class="badge-existing">live</span>' : '';
  let meta = '';
  if (isExchange) {
    meta = '<span class="node-tag ' + n.subtype + '">' + esc(n.subtype) + '</span>';
  } else {
    meta = '<span style="color:var(--muted);font-size:.72rem">size: ' + n.size + '</span>';
  }
  el.innerHTML =
    '<div class="node-header ' + headerClass + '">' + label + existBadge + '</div>' +
    '<div class="node-body">' +
      '<div class="node-name">' + esc(n.name) + '</div>' +
      '<div class="node-meta" style="margin-top:6px">' + meta + '</div>' +
    '</div>' +
    (isExchange ? '<div class="port out" data-port="out" data-node="'+n.id+'" title="Drag to connect"></div>' : '') +
    (!isExchange ? '<div class="port in" data-port="in" data-node="'+n.id+'" title="Connect here"></div>' : '');
}

// ─── Select ─────────────────────────────────────────────────
function selectNode(id) {
  selectedId = id;
  document.querySelectorAll('.node').forEach(el => el.classList.toggle('selected', parseInt(el.dataset.nodeId) === id));
  showProperties(id);
}
function deselectAll() {
  selectedId = null;
  document.querySelectorAll('.node.selected').forEach(el => el.classList.remove('selected'));
  document.getElementById('prop-panel').classList.remove('visible');
}

// ─── Properties Panel ───────────────────────────────────────
function showProperties(id) {
  const n = nodes.find(n => n.id === id);
  if (!n) return;
  const panel = document.getElementById('prop-panel');
  const content = document.getElementById('prop-content');
  panel.classList.add('visible');

  let html = '';
  if (n.type === 'exchange') {
    html += '<div class="prop-group"><label class="prop-label">Name</label>' +
      '<input class="prop-input" id="prop-name" value="' + escAttr(n.name) + '" ' + (n.existing ? 'disabled' : '') + '></div>';
    html += '<div class="prop-group"><label class="prop-label">Type</label>' +
      '<select class="prop-select" id="prop-subtype" ' + (n.existing ? 'disabled' : '') + '>' +
      '<option value="direct"' + (n.subtype==='direct'?' selected':'') + '>Direct</option>' +
      '<option value="fanout"' + (n.subtype==='fanout'?' selected':'') + '>Fanout</option>' +
      '<option value="topic"' + (n.subtype==='topic'?' selected':'') + '>Topic</option>' +
      '</select></div>';
  } else {
    html += '<div class="prop-group"><label class="prop-label">Name</label>' +
      '<input class="prop-input" id="prop-name" value="' + escAttr(n.name) + '" ' + (n.existing ? 'disabled' : '') + '></div>';
    html += '<div class="prop-group"><label class="prop-label">Buffer Size</label>' +
      '<input class="prop-input" id="prop-size" type="number" value="' + n.size + '" ' + (n.existing ? 'disabled' : '') + '></div>';
  }
  // Show connections for this node
  const conns = connections.filter(c => c.fromId === id || c.toId === id);
  if (conns.length) {
    html += '<div style="margin-top:12px"><label class="prop-label">Bindings</label>';
    conns.forEach(c => {
      const other = c.fromId === id ? nodes.find(nn=>nn.id===c.toId) : nodes.find(nn=>nn.id===c.fromId);
      html += '<div style="font-size:.78rem;color:var(--text2);padding:4px 0;display:flex;align-items:center;gap:6px">' +
        '<span style="color:var(--cyan);font-family:JetBrains Mono,monospace">' + esc(other?.name||'?') + '</span>' +
        (c.routingKey ? ' <span style="color:var(--muted)">key:</span> <span style="color:var(--amber);font-family:JetBrains Mono,monospace">' + esc(c.routingKey) + '</span>' : '') +
        '</div>';
    });
    html += '</div>';
  }
  if (!n.existing) {
    html += '<button class="btn-delete" onclick="deleteNode(' + id + ')">Delete Node</button>';
  }
  content.innerHTML = html;

  // Bind property changes
  const nameInput = document.getElementById('prop-name');
  if (nameInput && !n.existing) {
    nameInput.addEventListener('input', e => {
      n.name = e.target.value;
      const el = document.getElementById('node-' + n.id);
      if (el) updateNodeContent(el, n);
    });
  }
  const subtypeSelect = document.getElementById('prop-subtype');
  if (subtypeSelect && !n.existing) {
    subtypeSelect.addEventListener('change', e => {
      n.subtype = e.target.value;
      const el = document.getElementById('node-' + n.id);
      if (el) updateNodeContent(el, n);
    });
  }
  const sizeInput = document.getElementById('prop-size');
  if (sizeInput && !n.existing) {
    sizeInput.addEventListener('input', e => {
      n.size = parseInt(e.target.value) || 1000;
    });
  }
}

function deleteNode(id) {
  nodes = nodes.filter(n => n.id !== id);
  connections = connections.filter(c => c.fromId !== id && c.toId !== id);
  const el = document.getElementById('node-' + id);
  if (el) el.remove();
  deselectAll();
  renderConnections();
}

// ─── Drag ───────────────────────────────────────────────────
document.addEventListener('mousemove', e => {
  if (!dragState) return;
  const n = nodes.find(n => n.id === dragState.nodeId);
  if (!n) return;
  const x = e.clientX - dragState.wrapLeft + wrap.scrollLeft - dragState.offsetX;
  const y = e.clientY - dragState.wrapTop + wrap.scrollTop - dragState.offsetY;
  n.x = Math.max(0, x);
  n.y = Math.max(0, y);
  const el = document.getElementById('node-' + n.id);
  if (el) { el.style.left = n.x + 'px'; el.style.top = n.y + 'px'; }
  renderConnections();
});
document.addEventListener('mouseup', () => { dragState = null; });

// ─── Connections (port clicks) ──────────────────────────────
canvas.addEventListener('click', e => {
  const port = e.target.closest('.port');
  if (!port) {
    if (!e.target.closest('.node')) deselectAll();
    return;
  }
  const nodeId = parseInt(port.dataset.node);
  const portType = port.dataset.port;

  if (portType === 'out') {
    // Start connection from exchange
    const n = nodes.find(n => n.id === nodeId);
    if (!n || n.type !== 'exchange') return;
    connectingFrom = nodeId;
    document.querySelectorAll('.port.active').forEach(p => p.classList.remove('active'));
    port.classList.add('active');
  } else if (portType === 'in' && connectingFrom !== null) {
    // Complete connection to queue
    const n = nodes.find(n => n.id === nodeId);
    if (!n || n.type !== 'queue') return;
    promptRoutingKey(connectingFrom, nodeId);
    document.querySelectorAll('.port.active').forEach(p => p.classList.remove('active'));
    connectingFrom = null;
  }
});

// Escape cancels connection
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    connectingFrom = null;
    document.querySelectorAll('.port.active').forEach(p => p.classList.remove('active'));
  }
  if (e.key === 'Delete' && selectedId !== null) {
    const n = nodes.find(n => n.id === selectedId);
    if (n && !n.existing) deleteNode(selectedId);
  }
});

function promptRoutingKey(fromId, toId) {
  // Show inline input near the midpoint
  const fromNode = nodes.find(n => n.id === fromId);
  const toNode = nodes.find(n => n.id === toId);
  if (!fromNode || !toNode) return;
  // For fanout, routing key is not used
  if (fromNode.subtype === 'fanout') {
    addConnection(fromId, toId, '');
    return;
  }
  const mx = (fromNode.x + 180 + toNode.x) / 2;
  const my = (fromNode.y + 30 + toNode.y + 30) / 2;
  const input = document.createElement('input');
  input.className = 'conn-label-input';
  input.style.left = mx + 'px';
  input.style.top = my + 'px';
  input.placeholder = 'routing key';
  input.value = toNode.name; // default to queue name for direct
  canvas.appendChild(input);
  input.focus();
  input.select();
  function finish() {
    addConnection(fromId, toId, input.value);
    input.remove();
  }
  input.addEventListener('keydown', e => { if (e.key === 'Enter') finish(); if (e.key === 'Escape') input.remove(); });
  input.addEventListener('blur', finish);
}

// ─── Render SVG connections ─────────────────────────────────
function renderConnections() {
  let svg = '';
  connections.forEach(c => {
    const from = nodes.find(n => n.id === c.fromId);
    const to = nodes.find(n => n.id === c.toId);
    if (!from || !to) return;
    const fromEl = document.getElementById('node-' + from.id);
    const toEl = document.getElementById('node-' + to.id);
    if (!fromEl || !toEl) return;
    const x1 = from.x + fromEl.offsetWidth;
    const y1 = from.y + fromEl.offsetHeight / 2;
    const x2 = to.x;
    const y2 = to.y + toEl.offsetHeight / 2;
    const cpx = Math.abs(x2 - x1) * 0.5;
    const d = 'M'+x1+','+y1+' C'+(x1+cpx)+','+y1+' '+(x2-cpx)+','+y2+' '+x2+','+y2;
    svg += '<path d="'+d+'" />';
    // Label
    if (c.routingKey) {
      const mx = (x1+x2)/2, my = (y1+y2)/2 - 8;
      svg += '<text x="'+mx+'" y="'+my+'" text-anchor="middle">'+esc(c.routingKey)+'</text>';
    }
  });
  svgEl.innerHTML = svg;
}

// ─── Load existing topology ─────────────────────────────────
async function loadTopology() {
  try {
    const [queueList, exchangeList] = await Promise.all([
      fetch('/api/queues').then(r=>r.json()),
      fetch('/api/exchanges').then(r=>r.json()),
    ]);
    // Clear
    clearCanvas(true);
    const exMap = {}; // name -> node
    const qMap = {};  // name -> node
    // Create exchange nodes
    let exY = 80;
    (exchangeList||[]).forEach(ex => {
      const n = addExchangeNode(ex.name || '(default)', ex.type, 160, exY, true);
      n.realName = ex.name; // keep the real name (might be "")
      exMap[ex.name] = n;
      exY += 140;
    });
    // Create queue nodes
    let qY = 80;
    (queueList||[]).forEach(q => {
      const n = addQueueNode(q.name, q.capacity, 650, qY, true);
      qMap[q.name] = n;
      qY += 140;
    });
    // Create connections from exchange bindings
    (exchangeList||[]).forEach(ex => {
      const exNode = exMap[ex.name];
      if (!exNode) return;
      (ex.bindings||[]).forEach(b => {
        const qNode = qMap[b.queue_name];
        if (!qNode) return;
        const c = {id: nextId++, fromId: exNode.id, toId: qNode.id, routingKey: b.routing_key, existing: true};
        if (!connections.some(cc => cc.fromId===c.fromId && cc.toId===c.toId && cc.routingKey===c.routingKey)) {
          connections.push(c);
        }
      });
    });
    renderConnections();
    toast('Topology loaded', 'ok');
  } catch(err) {
    console.error(err);
    toast('Failed to load topology', 'err');
  }
}

// ─── Deploy ─────────────────────────────────────────────────
async function deploy() {
  const newExchanges = nodes.filter(n => n.type==='exchange' && !n.existing);
  const newQueues = nodes.filter(n => n.type==='queue' && !n.existing);
  const newConns = connections.filter(c => !c.existing);
  if (!newExchanges.length && !newQueues.length && !newConns.length) {
    toast('Nothing new to deploy', 'err');
    return;
  }
  let errors = 0;
  // 1. Create exchanges
  for (const n of newExchanges) {
    try {
      const res = await fetch('/api/declare-exchange', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({name: n.name, type: n.subtype})
      });
      if (!res.ok) { const d = await res.json(); toast('Exchange: ' + d.error, 'err'); errors++; continue; }
      n.existing = true;
      const el = document.getElementById('node-' + n.id);
      if (el) updateNodeContent(el, n);
    } catch(e) { errors++; toast('Network error', 'err'); }
  }
  // 2. Create queues
  for (const n of newQueues) {
    try {
      const res = await fetch('/api/declare-queue', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({name: n.name, size: n.size})
      });
      if (!res.ok) { const d = await res.json(); toast('Queue: ' + d.error, 'err'); errors++; continue; }
      n.existing = true;
      const el = document.getElementById('node-' + n.id);
      if (el) updateNodeContent(el, n);
    } catch(e) { errors++; toast('Network error', 'err'); }
  }
  // 3. Create bindings
  for (const c of newConns) {
    const fromNode = nodes.find(n => n.id === c.fromId);
    const toNode = nodes.find(n => n.id === c.toId);
    if (!fromNode || !toNode) continue;
    const exName = fromNode.realName !== undefined ? fromNode.realName : fromNode.name;
    try {
      const res = await fetch('/api/bind-queue', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({queue_name: toNode.name, exchange: exName, routing_key: c.routingKey})
      });
      if (!res.ok) { const d = await res.json(); toast('Bind: ' + d.error, 'err'); errors++; continue; }
      c.existing = true;
    } catch(e) { errors++; toast('Network error', 'err'); }
  }
  if (!errors) {
    toast('Topology deployed successfully!', 'ok');
  } else {
    toast('Deployed with ' + errors + ' error(s)', 'err');
  }
  if (selectedId) showProperties(selectedId);
}

// ─── Clear ──────────────────────────────────────────────────
function clearCanvas(silent) {
  nodes = [];
  connections = [];
  document.querySelectorAll('.node').forEach(el => el.remove());
  svgEl.innerHTML = '';
  deselectAll();
  if (!silent) toast('Canvas cleared', 'ok');
}

// ─── Helpers ────────────────────────────────────────────────
function esc(s) { if(!s) return ''; const d=document.createElement('div'); d.textContent=String(s); return d.innerHTML; }
function escAttr(s) { return String(s).replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
function toast(msg, type) {
  const el = document.createElement('div');
  el.className = 'toast toast-' + type;
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(()=>{el.style.opacity='0';el.style.transition='opacity .3s'},2200);
  setTimeout(()=>el.remove(),2600);
}
</script>
</body>
</html>`
