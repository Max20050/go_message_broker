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
.canvas-wrap{position:fixed;top:56px;left:260px;right:0;bottom:0;overflow:auto;z-index:1}
.canvas{position:relative;width:4000px;height:4000px;transform-origin:0 0;transition:transform .15s ease}
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

/* ── Zoom controls ── */
.zoom-controls{display:flex;align-items:center;gap:4px;border:1px solid var(--border);border-radius:8px;padding:2px;background:rgba(0,0,0,.2)}
.zoom-btn{width:28px;height:28px;border:none;border-radius:6px;background:transparent;color:var(--text2);font-family:inherit;font-size:1rem;font-weight:600;cursor:pointer;display:grid;place-items:center;transition:.15s}
.zoom-btn:hover{background:var(--bg-card);color:var(--text)}
.zoom-label{font-family:'JetBrains Mono',monospace;font-size:.72rem;color:var(--text2);min-width:42px;text-align:center;user-select:none;cursor:pointer}
.zoom-label:hover{color:var(--text)}

/* ── Help modal ── */
.help-overlay{display:none;position:fixed;inset:0;z-index:300;background:rgba(0,0,0,.6);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);justify-content:center;align-items:flex-start;padding:40px 20px;overflow-y:auto}
.help-overlay.open{display:flex}
.help-modal{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);box-shadow:0 16px 60px rgba(0,0,0,.5);width:100%;max-width:760px;animation:helpIn .3s ease}
@keyframes helpIn{from{opacity:0;transform:translateY(-16px)}to{opacity:1;transform:translateY(0)}}
.help-header{display:flex;align-items:center;justify-content:space-between;padding:22px 28px;border-bottom:1px solid var(--border)}
.help-header h2{font-size:1.1rem;font-weight:700;display:flex;align-items:center;gap:10px}
.help-close{background:none;border:none;color:var(--muted);font-size:1.4rem;cursor:pointer;padding:4px 8px;border-radius:6px;transition:.2s}
.help-close:hover{color:var(--text);background:rgba(255,255,255,.06)}
.help-body{padding:28px;max-height:75vh;overflow-y:auto}
.help-section{margin-bottom:28px}
.help-section:last-child{margin-bottom:0}
.help-section h3{font-size:.92rem;font-weight:700;margin-bottom:12px;display:flex;align-items:center;gap:8px;color:var(--text)}
.help-section p{font-size:.84rem;color:var(--text2);line-height:1.7;margin-bottom:10px}
.help-section p:last-child{margin-bottom:0}
.help-card{background:rgba(0,0,0,.15);border:1px solid var(--border);border-radius:10px;padding:14px 18px;margin-bottom:10px}
.help-card .hc-title{font-size:.82rem;font-weight:700;margin-bottom:4px;display:flex;align-items:center;gap:8px}
.help-card .hc-title .dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.help-card p{font-size:.78rem;color:var(--text2);line-height:1.6;margin:0}
.help-table{width:100%;border-collapse:collapse;font-size:.78rem;margin-top:6px}
.help-table th{text-align:left;padding:6px 10px;font-size:.68rem;color:var(--muted);text-transform:uppercase;letter-spacing:.6px;border-bottom:1px solid var(--border)}
.help-table td{padding:6px 10px;color:var(--text2);border-bottom:1px solid rgba(42,53,80,.3)}
.help-table td:first-child{color:var(--cyan);font-family:'JetBrains Mono',monospace;font-size:.76rem;white-space:nowrap}
.help-step{display:flex;gap:14px;margin-bottom:14px;align-items:flex-start}
.help-step .step-num{width:26px;height:26px;border-radius:50%;background:var(--grad-blue);color:#fff;font-size:.72rem;font-weight:700;display:grid;place-items:center;flex-shrink:0;margin-top:1px}
.help-step .step-text{font-size:.82rem;color:var(--text2);line-height:1.6}
.help-step .step-text strong{color:var(--text);font-weight:600}
.help-link{font-family:inherit;font-size:.78rem;padding:5px 14px;border:1px solid var(--border);border-radius:7px;background:transparent;color:var(--text2);cursor:pointer;transition:.2s;text-decoration:none;display:inline-flex;align-items:center;gap:5px}
.help-link:hover{background:var(--bg-hover);color:var(--text);border-color:var(--blue)}
</style>
</head>
<body>

<!-- NAV -->
<nav class="nav">
  <div class="nav-brand">
    <div class="logo">MQ</div>
    <span>Topology Editor</span>
  </div>
  <div style="display:flex;align-items:center;gap:12px">
    <div class="zoom-controls">
      <button class="zoom-btn" onclick="zoomOut()" title="Zoom out">−</button>
      <span class="zoom-label" id="zoom-label" onclick="resetZoom()" title="Click to reset">100%</span>
      <button class="zoom-btn" onclick="zoomIn()" title="Zoom in">+</button>
    </div>
    <div class="nav-links">
      <button class="nav-link" onclick="toggleHelp()" title="How to use the editor">? Help</button>
      <a class="nav-link" href="/">← Dashboard</a>
      <button class="nav-link" onclick="loadTopology()" title="Import current broker state">⟳ Load Topology</button>
      <button class="nav-link" onclick="clearCanvas()">✕ Clear</button>
      <button class="nav-link primary" onclick="deploy()">🚀 Deploy</button>
    </div>
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

<!-- HELP MODAL -->
<div class="help-overlay" id="help-overlay">
  <div class="help-modal">
    <div class="help-header">
      <h2>📖 Topology Editor Guide</h2>
      <button class="help-close" onclick="toggleHelp()" title="Close">✕</button>
    </div>
    <div class="help-body">

      <!-- What is this? -->
      <div class="help-section">
        <h3>🎯 What is the Topology Editor?</h3>
        <p>The Topology Editor lets you visually design your message broker architecture. You can create <strong>exchanges</strong> and <strong>queues</strong>, draw <strong>bindings</strong> between them, and <strong>deploy</strong> the entire topology to the live broker with one click.</p>
        <p>Think of it as a whiteboard where you sketch how messages should flow through your system — then press a button to make it real.</p>
      </div>

      <!-- Exchanges -->
      <div class="help-section">
        <h3>🔀 Exchanges</h3>
        <p>An <strong>exchange</strong> receives messages from publishers and routes them to one or more queues based on rules. Every message is published to an exchange, never directly to a queue.</p>
        <p>There are three types of exchanges:</p>
        <div class="help-card">
          <div class="hc-title"><div class="dot" style="background:var(--blue)"></div> Direct</div>
          <p>Routes messages to queues whose <strong>binding key exactly matches</strong> the message's routing key. Use this for point-to-point messaging where each message goes to one specific queue.</p>
          <p style="margin-top:4px;color:var(--muted);font-size:.72rem">Example: routing key <span style="color:var(--cyan)">"payments.due"</span> → only queues bound with key <span style="color:var(--cyan)">"payments.due"</span></p>
        </div>
        <div class="help-card">
          <div class="hc-title"><div class="dot" style="background:var(--purple)"></div> Fanout</div>
          <p>Delivers every message to <strong>ALL bound queues</strong>, ignoring the routing key entirely. Use this for broadcasting — every subscriber gets every message.</p>
          <p style="margin-top:4px;color:var(--muted);font-size:.72rem">Example: a "logs" fanout exchange sends every log to console, file, and database queues simultaneously.</p>
        </div>
        <div class="help-card">
          <div class="hc-title"><div class="dot" style="background:var(--amber)"></div> Topic</div>
          <p>Routes messages using <strong>pattern matching</strong> on the routing key. Words are separated by dots. Use <span style="color:var(--cyan)">*</span> to match exactly one word and <span style="color:var(--cyan)">#</span> to match zero or more words.</p>
          <p style="margin-top:4px;color:var(--muted);font-size:.72rem">Example: pattern <span style="color:var(--cyan)">"payments.*"</span> matches <span style="color:var(--cyan)">"payments.due"</span> and <span style="color:var(--cyan)">"payments.received"</span> but not <span style="color:var(--cyan)">"orders.new"</span></p>
        </div>
      </div>

      <!-- Queues -->
      <div class="help-section">
        <h3>📦 Queues</h3>
        <p>A <strong>queue</strong> is a buffer that stores messages until a consumer picks them up. Messages are delivered in order (FIFO) and each message is delivered to exactly one consumer.</p>
        <div class="help-card">
          <div class="hc-title"><div class="dot" style="background:var(--cyan)"></div> Buffer Size</div>
          <p>The buffer size determines how many messages the queue can hold in its fast channel. Messages beyond this limit go to an overflow list. Default is <strong>1000</strong>. Set higher for high-throughput queues.</p>
        </div>
        <div class="help-card">
          <div class="hc-title"><div class="dot" style="background:var(--emerald)"></div> Consumers</div>
          <p>Consumers are clients subscribed to a queue. When a message arrives, the broker delivers it to one available consumer. If no consumers are connected, messages wait in the queue.</p>
        </div>
      </div>

      <!-- Bindings -->
      <div class="help-section">
        <h3>🔗 Bindings &amp; Routing Keys</h3>
        <p>A <strong>binding</strong> is a link between an exchange and a queue. It tells the exchange: "send matching messages to this queue."</p>
        <p>The <strong>routing key</strong> is a string attached to each binding that determines which messages the queue receives:</p>
        <table class="help-table">
          <tr><th>Exchange Type</th><th>How Routing Key Works</th></tr>
          <tr><td>Direct</td><td>Must exactly match the message's routing key</td></tr>
          <tr><td>Fanout</td><td>Ignored — all bound queues get every message</td></tr>
          <tr><td>Topic</td><td>Pattern matched with <code style="color:var(--cyan)">*</code> (one word) and <code style="color:var(--cyan)">#</code> (zero+ words)</td></tr>
        </table>
      </div>

      <!-- How to use -->
      <div class="help-section">
        <h3>🛠️ How to Use the Editor</h3>
        <div class="help-step">
          <div class="step-num">1</div>
          <div class="step-text"><strong>Add an Exchange</strong> — Click the "Exchange" button in the left sidebar. A new node appears on the canvas. Click it to set its <strong>name</strong> and <strong>type</strong> (direct, fanout, or topic) in the properties panel.</div>
        </div>
        <div class="help-step">
          <div class="step-num">2</div>
          <div class="step-text"><strong>Add Queues</strong> — Click the "Queue" button to add queue nodes. Set the <strong>name</strong> and <strong>buffer size</strong> in the properties panel.</div>
        </div>
        <div class="help-step">
          <div class="step-num">3</div>
          <div class="step-text"><strong>Create Bindings</strong> — Click the <strong style="color:var(--blue)">circle on the right</strong> of an exchange (output port), then click the <strong style="color:var(--blue)">circle on the left</strong> of a queue (input port). For direct/topic exchanges, you’ll be asked for a routing key. For fanout, the binding is created immediately.</div>
        </div>
        <div class="help-step">
          <div class="step-num">4</div>
          <div class="step-text"><strong>Arrange</strong> — Drag nodes to organize your diagram. Use <strong>Ctrl + Scroll</strong> or the zoom buttons to zoom in/out. Scroll to pan around.</div>
        </div>
        <div class="help-step">
          <div class="step-num">5</div>
          <div class="step-text"><strong>Deploy</strong> — When you’re happy with the layout, click <strong style="color:var(--emerald)">🚀 Deploy</strong>. This creates all new exchanges, queues, and bindings on the live broker. Deployed nodes show a green <span class="badge-existing" style="display:inline">LIVE</span> badge and can no longer be edited.</div>
        </div>
      </div>

      <!-- Load Topology -->
      <div class="help-section">
        <h3>⟳ Loading Existing Topology</h3>
        <p>Click <strong>⟳ Load Topology</strong> in the navbar to import the broker's current state into the canvas. This lets you see what already exists and add new resources alongside live ones.</p>
        <p>Live nodes appear with a <span class="badge-existing" style="display:inline">LIVE</span> badge. They cannot be renamed or deleted from the editor — they represent real resources on the broker.</p>
      </div>

      <!-- Keyboard shortcuts -->
      <div class="help-section">
        <h3>⌨️ Keyboard Shortcuts</h3>
        <table class="help-table">
          <tr><th>Shortcut</th><th>Action</th></tr>
          <tr><td>Ctrl + Scroll</td><td>Zoom in / out (centered on cursor)</td></tr>
          <tr><td>Ctrl + =</td><td>Zoom in</td></tr>
          <tr><td>Ctrl + -</td><td>Zoom out</td></tr>
          <tr><td>Ctrl + 0</td><td>Reset zoom to 100%</td></tr>
          <tr><td>Delete</td><td>Delete selected node</td></tr>
          <tr><td>Escape</td><td>Cancel connection or close help</td></tr>
        </table>
      </div>

      <!-- Important note -->
      <div class="help-section">
        <h3>⚠️ Important Notes</h3>
        <p>• Queues created from the editor are <strong>NOT</strong> auto-bound to the default exchange. They only get the bindings you explicitly draw. Queues created by TCP clients (code) still auto-bind for point-to-point messaging.</p>
        <p>• The editor does not delete existing resources. It only creates new ones. To manage existing queues and messages, use the <a href="/" style="color:var(--blue)">Dashboard</a>.</p>
        <p>• After deploying, you can connect consumers and publishers to the new queues using the Go client library.</p>
      </div>

    </div>
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
let currentZoom = 1;
const ZOOM_MIN = 0.25;
const ZOOM_MAX = 3;
const ZOOM_STEP = 0.15;

const canvas = document.getElementById('canvas');
const wrap = document.getElementById('canvas-wrap');
const svgEl = document.getElementById('svg-connections');

// ─── Zoom ───────────────────────────────────────────────────
function setZoom(z, centerX, centerY) {
  const oldZoom = currentZoom;
  currentZoom = Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, z));
  canvas.style.transform = 'scale(' + currentZoom + ')';
  document.getElementById('zoom-label').textContent = Math.round(currentZoom * 100) + '%';
  // Adjust scroll to keep the zoom centered
  if (centerX !== undefined && centerY !== undefined) {
    wrap.scrollLeft = (wrap.scrollLeft + centerX) * (currentZoom / oldZoom) - centerX;
    wrap.scrollTop  = (wrap.scrollTop + centerY) * (currentZoom / oldZoom) - centerY;
  }
}
function zoomIn()  { setZoom(currentZoom + ZOOM_STEP); }
function zoomOut() { setZoom(currentZoom - ZOOM_STEP); }
function resetZoom() { setZoom(1); }

// Ctrl + scroll wheel to zoom
wrap.addEventListener('wheel', e => {
  if (!e.ctrlKey && !e.metaKey) return; // normal scroll pans
  e.preventDefault();
  const rect = wrap.getBoundingClientRect();
  const cx = e.clientX - rect.left;
  const cy = e.clientY - rect.top;
  const delta = e.deltaY > 0 ? -ZOOM_STEP : ZOOM_STEP;
  setZoom(currentZoom + delta, cx, cy);
}, {passive: false});

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
      // Offset in canvas-space (divide by zoom)
      offsetX: (e.clientX - rect.left) / currentZoom,
      offsetY: (e.clientY - rect.top) / currentZoom,
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
  // Divide by zoom so pixel movement maps correctly to canvas coords
  const x = (e.clientX - dragState.wrapLeft + wrap.scrollLeft) / currentZoom - dragState.offsetX;
  const y = (e.clientY - dragState.wrapTop + wrap.scrollTop) / currentZoom - dragState.offsetY;
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

// Escape cancels connection, Delete removes node, Ctrl+/- zooms
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    connectingFrom = null;
    document.querySelectorAll('.port.active').forEach(p => p.classList.remove('active'));
    // Also close help if open
    const helpEl = document.getElementById('help-overlay');
    if (helpEl.classList.contains('open')) helpEl.classList.remove('open');
  }
  if (e.key === 'Delete' && selectedId !== null) {
    const n = nodes.find(n => n.id === selectedId);
    if (n && !n.existing) deleteNode(selectedId);
  }
  // Zoom keyboard shortcuts
  if ((e.ctrlKey || e.metaKey) && (e.key === '=' || e.key === '+')) { e.preventDefault(); zoomIn(); }
  if ((e.ctrlKey || e.metaKey) && e.key === '-') { e.preventDefault(); zoomOut(); }
  if ((e.ctrlKey || e.metaKey) && e.key === '0') { e.preventDefault(); resetZoom(); }
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

// ─── Help modal ─────────────────────────────────────────────
function toggleHelp() {
  document.getElementById('help-overlay').classList.toggle('open');
}
document.getElementById('help-overlay').addEventListener('click', e => {
  if (e.target === e.currentTarget) toggleHelp();
});
</script>
</body>
</html>`
