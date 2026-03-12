package admin

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GoMQ – Admin Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        /* ───────── Reset & Variables ───────── */
        *, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }

        :root {
            --bg-primary:    #0a0e17;
            --bg-secondary:  #111827;
            --bg-card:       #1a2236;
            --bg-card-hover: #1f2942;
            --border:        #2a3550;
            --text-primary:  #e2e8f0;
            --text-secondary:#94a3b8;
            --text-muted:    #64748b;
            --accent-blue:   #3b82f6;
            --accent-purple: #8b5cf6;
            --accent-cyan:   #06b6d4;
            --accent-emerald:#10b981;
            --accent-amber:  #f59e0b;
            --accent-rose:   #f43f5e;

            --gradient-blue:   linear-gradient(135deg, #3b82f6, #2563eb);
            --gradient-purple: linear-gradient(135deg, #8b5cf6, #7c3aed);
            --gradient-cyan:   linear-gradient(135deg, #06b6d4, #0891b2);
            --gradient-emerald:linear-gradient(135deg, #10b981, #059669);

            --shadow-card: 0 4px 24px rgba(0,0,0,.35);
            --shadow-glow: 0 0 30px rgba(59,130,246,.12);
            --radius:      16px;
            --radius-sm:   10px;
        }

        html { scroll-behavior:smooth; }

        body {
            font-family: 'Inter', -apple-system, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height:100vh;
            overflow-x:hidden;
        }

        /* ───────── Background glow ───────── */
        body::before {
            content:'';
            position:fixed; inset:0;
            background:
                radial-gradient(ellipse 800px 600px at 20% 10%, rgba(59,130,246,.07), transparent),
                radial-gradient(ellipse 600px 500px at 80% 80%, rgba(139,92,246,.06), transparent);
            pointer-events:none; z-index:0;
        }

        /* ───────── Nav ───────── */
        .nav {
            position:sticky; top:0; z-index:100;
            backdrop-filter:blur(20px) saturate(180%);
            -webkit-backdrop-filter:blur(20px) saturate(180%);
            background:rgba(10,14,23,.75);
            border-bottom:1px solid var(--border);
            padding:0 32px;
        }
        .nav-inner {
            max-width:1400px; margin:auto;
            display:flex; align-items:center; justify-content:space-between;
            height:64px;
        }
        .nav-brand {
            display:flex; align-items:center; gap:12px;
            font-weight:700; font-size:1.15rem; letter-spacing:-.5px;
        }
        .nav-brand .logo {
            width:36px; height:36px; border-radius:10px;
            background:var(--gradient-blue);
            display:grid; place-items:center;
            font-size:1rem; font-weight:800; color:#fff;
            box-shadow:0 0 20px rgba(59,130,246,.3);
        }
        .nav-status {
            display:flex; align-items:center; gap:8px;
            font-size:.82rem; color:var(--text-secondary);
        }
        .status-dot {
            width:8px; height:8px; border-radius:50%;
            background:#10b981;
            box-shadow:0 0 8px rgba(16,185,129,.6);
            animation:pulse 2s ease-in-out infinite;
        }
        @keyframes pulse {
            0%,100%{opacity:1;transform:scale(1);}
            50%{opacity:.6;transform:scale(1.3);}
        }
        .btn-refresh {
            background:transparent; border:1px solid var(--border);
            color:var(--text-secondary); border-radius:8px;
            padding:6px 14px; font-size:.8rem; cursor:pointer;
            font-family:inherit; transition:.2s;
        }
        .btn-refresh:hover { background:var(--bg-card); color:var(--text-primary); border-color:var(--accent-blue); }

        /* ───────── Layout ───────── */
        .container { max-width:1400px; margin:auto; padding:28px 32px 60px; position:relative; z-index:1; }

        /* ───────── Stats Row ───────── */
        .stats-row {
            display:grid; grid-template-columns:repeat(4,1fr); gap:20px;
            margin-bottom:32px;
        }
        .stat-card {
            background:var(--bg-card);
            border:1px solid var(--border);
            border-radius:var(--radius);
            padding:22px 24px;
            display:flex; align-items:center; gap:18px;
            box-shadow:var(--shadow-card);
            transition:transform .25s ease, box-shadow .25s ease, border-color .25s ease;
        }
        .stat-card:hover { transform:translateY(-4px); box-shadow:var(--shadow-glow); border-color:var(--accent-blue); }
        .stat-icon {
            width:50px; height:50px; border-radius:14px;
            display:grid; place-items:center;
            font-size:1.35rem;
            flex-shrink:0;
        }
        .stat-icon.blue   { background:rgba(59,130,246,.15); color:var(--accent-blue); }
        .stat-icon.purple { background:rgba(139,92,246,.15); color:var(--accent-purple); }
        .stat-icon.cyan   { background:rgba(6,182,212,.15);  color:var(--accent-cyan); }
        .stat-icon.emerald{ background:rgba(16,185,129,.15); color:var(--accent-emerald); }
        .stat-value { font-size:1.8rem; font-weight:700; line-height:1; }
        .stat-label { font-size:.8rem; color:var(--text-muted); margin-top:3px; text-transform:uppercase; letter-spacing:.6px; }

        /* ───────── Tabs ───────── */
        .tab-bar {
            display:flex; gap:4px;
            background:var(--bg-secondary);
            border:1px solid var(--border);
            border-radius:var(--radius-sm);
            padding:4px; margin-bottom:24px;
            width:fit-content;
        }
        .tab-btn {
            font-family:inherit; font-size:.85rem; font-weight:500;
            padding:8px 22px; border:none; border-radius:8px;
            background:transparent; color:var(--text-secondary);
            cursor:pointer; transition:.2s;
        }
        .tab-btn:hover { color:var(--text-primary); }
        .tab-btn.active {
            background:var(--accent-blue);
            color:#fff;
            box-shadow:0 2px 12px rgba(59,130,246,.35);
        }

        /* ───────── Table cards ───────── */
        .panel {
            background:var(--bg-card);
            border:1px solid var(--border);
            border-radius:var(--radius);
            box-shadow:var(--shadow-card);
            overflow:hidden;
            animation:fadeIn .3s ease;
        }
        @keyframes fadeIn { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }

        .panel-header {
            padding:18px 24px;
            display:flex; align-items:center; justify-content:space-between;
            border-bottom:1px solid var(--border);
        }
        .panel-title { font-size:1rem; font-weight:600; }
        .panel-badge {
            font-family:'JetBrains Mono',monospace;
            font-size:.72rem; font-weight:500;
            padding:3px 10px; border-radius:20px;
            background:rgba(59,130,246,.15); color:var(--accent-blue);
        }

        table { width:100%; border-collapse:collapse; }
        thead th {
            text-align:left; padding:12px 24px;
            font-size:.72rem; font-weight:600;
            color:var(--text-muted); text-transform:uppercase;
            letter-spacing:.8px; border-bottom:1px solid var(--border);
            background:rgba(0,0,0,.15);
        }
        tbody tr {
            border-bottom:1px solid rgba(42,53,80,.4);
            transition:background .15s;
        }
        tbody tr:last-child { border-bottom:none; }
        tbody tr:hover { background:var(--bg-card-hover); }
        tbody td {
            padding:14px 24px; font-size:.88rem;
            color:var(--text-secondary);
        }

        .mono { font-family:'JetBrains Mono',monospace; font-size:.82rem; color:var(--accent-cyan); }

        .tag {
            display:inline-block; padding:2px 10px;
            border-radius:6px; font-size:.72rem; font-weight:600;
            text-transform:uppercase; letter-spacing:.5px;
        }
        .tag-direct  { background:rgba(59,130,246,.15);  color:var(--accent-blue); }
        .tag-fanout  { background:rgba(139,92,246,.15);  color:var(--accent-purple); }
        .tag-topic   { background:rgba(245,158,11,.15);  color:var(--accent-amber); }
        .tag-true    { background:rgba(16,185,129,.15);  color:var(--accent-emerald); }
        .tag-false   { background:rgba(244,63,94,.12);   color:var(--accent-rose); }
        .tag-queued  { background:rgba(59,130,246,.12);  color:var(--accent-blue); }
        .tag-inflight{ background:rgba(245,158,11,.14);  color:var(--accent-amber); }

        .progress-bar-bg {
            width:100px; height:6px; border-radius:3px;
            background:rgba(255,255,255,.06);
            overflow:hidden; display:inline-block; vertical-align:middle;
            margin-left:8px;
        }
        .progress-bar-fill {
            height:100%; border-radius:3px;
            background:var(--gradient-blue);
            transition:width .4s ease;
        }

        .empty-state {
            padding:48px 24px; text-align:center;
            color:var(--text-muted); font-size:.9rem;
        }
        .empty-state span { font-size:2rem; display:block; margin-bottom:8px; }

        .hidden { display:none; }

        /* ───────── Clickable queue name ───────── */
        .queue-link {
            cursor:pointer; text-decoration:none;
            border-bottom:1px dashed var(--accent-cyan);
            transition:border-color .2s, color .2s;
        }
        .queue-link:hover {
            color:#fff;
            border-bottom-color:#fff;
        }

        /* ───────── Modal / Drawer ───────── */
        .modal-overlay {
            display:none;
            position:fixed; inset:0; z-index:200;
            background:rgba(0,0,0,.6);
            backdrop-filter:blur(6px);
            -webkit-backdrop-filter:blur(6px);
            justify-content:center; align-items:flex-start;
            padding:60px 20px;
            overflow-y:auto;
        }
        .modal-overlay.open {
            display:flex;
        }
        .modal {
            background:var(--bg-card);
            border:1px solid var(--border);
            border-radius:var(--radius);
            box-shadow:0 12px 48px rgba(0,0,0,.5);
            width:100%; max-width:1000px;
            animation:slideDown .3s ease;
        }
        @keyframes slideDown { from{opacity:0;transform:translateY(-20px)} to{opacity:1;transform:translateY(0)} }

        .modal-header {
            display:flex; align-items:center; justify-content:space-between;
            padding:20px 24px;
            border-bottom:1px solid var(--border);
        }
        .modal-header h2 {
            font-size:1.05rem; font-weight:600;
            display:flex; align-items:center; gap:10px;
        }
        .modal-close {
            background:none; border:none; color:var(--text-muted);
            font-size:1.3rem; cursor:pointer; padding:4px 8px;
            border-radius:6px; transition:.2s;
        }
        .modal-close:hover { color:var(--text-primary); background:rgba(255,255,255,.06); }

        .modal-body { padding:0; max-height:65vh; overflow-y:auto; }

        .modal-body table { margin:0; }

        /* ───────── ACK button ───────── */
        .btn-ack {
            font-family:inherit; font-size:.72rem; font-weight:600;
            padding:4px 14px; border:none; border-radius:6px;
            background:var(--gradient-emerald);
            color:#fff; cursor:pointer;
            transition:transform .15s, box-shadow .15s;
            text-transform:uppercase; letter-spacing:.5px;
        }
        .btn-ack:hover {
            transform:translateY(-1px);
            box-shadow:0 4px 12px rgba(16,185,129,.35);
        }
        .btn-ack:active { transform:translateY(0); }
        .btn-ack:disabled {
            opacity:.4; cursor:not-allowed;
            transform:none; box-shadow:none;
        }

        /* ───────── Payload preview ───────── */
        .payload-preview {
            max-width:320px;
            overflow:hidden; text-overflow:ellipsis; white-space:nowrap;
            font-family:'JetBrains Mono',monospace; font-size:.78rem;
            color:var(--text-secondary);
            cursor:pointer;
            transition:color .15s;
        }
        .payload-preview:hover { color:var(--text-primary); }

        /* ───────── Payload expanded ───────── */
        .payload-full {
            display:none;
            position:fixed; z-index:300;
            background:var(--bg-secondary);
            border:1px solid var(--border);
            border-radius:12px;
            padding:16px 20px;
            box-shadow:0 8px 32px rgba(0,0,0,.5);
            max-width:500px; max-height:300px;
            overflow:auto;
            font-family:'JetBrains Mono',monospace; font-size:.78rem;
            color:var(--accent-cyan);
            white-space:pre-wrap; word-break:break-all;
        }

        /* ───────── Toast notification ───────── */
        .toast {
            position:fixed; bottom:28px; right:28px; z-index:400;
            padding:12px 24px;
            border-radius:10px;
            font-size:.85rem; font-weight:500;
            color:#fff;
            box-shadow:0 6px 24px rgba(0,0,0,.4);
            animation:toastIn .3s ease;
            pointer-events:none;
        }
        .toast-success { background:linear-gradient(135deg, #10b981, #059669); }
        .toast-error   { background:linear-gradient(135deg, #f43f5e, #e11d48); }
        @keyframes toastIn { from{opacity:0;transform:translateY(12px)} to{opacity:1;transform:translateY(0)} }

        /* ───────── Responsive ───────── */
        @media (max-width:900px) {
            .stats-row { grid-template-columns:repeat(2,1fr); }
            .container { padding:20px 16px 40px; }
        }
        @media (max-width:600px) {
            .stats-row { grid-template-columns:1fr; }
            .nav { padding:0 16px; }
            table { font-size:.82rem; }
            thead th, tbody td { padding:10px 14px; }
        }
    </style>
</head>
<body>

<!-- ─── Navbar ─── -->
<nav class="nav">
    <div class="nav-inner">
        <div class="nav-brand">
            <div class="logo">MQ</div>
            <span>GoMQ Admin</span>
        </div>
        <div style="display:flex;align-items:center;gap:16px;">
            <div class="nav-status">
                <div class="status-dot"></div>
                <span>Broker Online</span>
            </div>
            <button class="btn-refresh" onclick="fetchAll()" title="Refresh now">↻ Refresh</button>
        </div>
    </div>
</nav>

<!-- ─── Main ─── -->
<div class="container">

    <!-- ── Stats Row ── -->
    <div class="stats-row">
        <div class="stat-card">
            <div class="stat-icon blue">📦</div>
            <div><div class="stat-value" id="st-queues">–</div><div class="stat-label">Queues</div></div>
        </div>
        <div class="stat-card">
            <div class="stat-icon purple">🔀</div>
            <div><div class="stat-value" id="st-exchanges">–</div><div class="stat-label">Exchanges</div></div>
        </div>
        <div class="stat-card">
            <div class="stat-icon cyan">🎧</div>
            <div><div class="stat-value" id="st-consumers">–</div><div class="stat-label">Consumers</div></div>
        </div>
        <div class="stat-card">
            <div class="stat-icon emerald">✉️</div>
            <div><div class="stat-value" id="st-messages">–</div><div class="stat-label">Messages</div></div>
        </div>
    </div>

    <!-- ── Tabs ── -->
    <div class="tab-bar" id="tab-bar">
        <button class="tab-btn active" data-tab="queues">Queues</button>
        <button class="tab-btn" data-tab="exchanges">Exchanges</button>
        <button class="tab-btn" data-tab="consumers">Consumers</button>
    </div>

    <!-- ── Panels ── -->

    <!-- Queues -->
    <div class="panel" id="panel-queues">
        <div class="panel-header">
            <span class="panel-title">Queues</span>
            <span class="panel-badge" id="badge-queues">0</span>
        </div>
        <div id="table-queues"></div>
    </div>

    <!-- Exchanges -->
    <div class="panel hidden" id="panel-exchanges">
        <div class="panel-header">
            <span class="panel-title">Exchanges</span>
            <span class="panel-badge" id="badge-exchanges">0</span>
        </div>
        <div id="table-exchanges"></div>
    </div>

    <!-- Consumers -->
    <div class="panel hidden" id="panel-consumers">
        <div class="panel-header">
            <span class="panel-title">Consumers</span>
            <span class="panel-badge" id="badge-consumers">0</span>
        </div>
        <div id="table-consumers"></div>
    </div>
</div>

<!-- ─── Messages Modal ─── -->
<div class="modal-overlay" id="msg-modal">
    <div class="modal">
        <div class="modal-header">
            <h2>✉️ Messages in <span class="mono" id="modal-queue-name"></span></h2>
            <button class="modal-close" id="modal-close" title="Close">✕</button>
        </div>
        <div class="modal-body" id="modal-body">
            <div class="empty-state"><span>⏳</span>Loading…</div>
        </div>
    </div>
</div>

<!-- ─── Payload popover ─── -->
<div class="payload-full" id="payload-popover"></div>

<script>
// ─── State ──────────────────────────────────────────────────
let currentModalQueue = null;

// ─── Tabs ───────────────────────────────────────────────────
document.getElementById('tab-bar').addEventListener('click', e => {
    if (!e.target.classList.contains('tab-btn')) return;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    e.target.classList.add('active');
    const tab = e.target.dataset.tab;
    ['queues','exchanges','consumers'].forEach(t => {
        document.getElementById('panel-'+t).classList.toggle('hidden', t !== tab);
    });
});

// ─── Modal controls ─────────────────────────────────────────
document.getElementById('modal-close').addEventListener('click', closeModal);
document.getElementById('msg-modal').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeModal();
});
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

function openModal(queueName) {
    currentModalQueue = queueName;
    document.getElementById('modal-queue-name').textContent = queueName;
    document.getElementById('modal-body').innerHTML = '<div class="empty-state"><span>⏳</span>Loading…</div>';
    document.getElementById('msg-modal').classList.add('open');
    document.body.style.overflow = 'hidden';
    fetchMessages(queueName);
}
function closeModal() {
    document.getElementById('msg-modal').classList.remove('open');
    document.body.style.overflow = '';
    currentModalQueue = null;
    hidePayloadPopover();
}

// ─── Data fetching ──────────────────────────────────────────
async function fetchJSON(url) {
    const res = await fetch(url);
    return res.json();
}

async function fetchAll() {
    try {
        const [overview, queues, exchanges, consumers] = await Promise.all([
            fetchJSON('/api/overview'),
            fetchJSON('/api/queues'),
            fetchJSON('/api/exchanges'),
            fetchJSON('/api/consumers'),
        ]);
        renderOverview(overview);
        renderQueues(queues || []);
        renderExchanges(exchanges || []);
        renderConsumers(consumers || []);
    } catch(err) {
        console.error('Fetch error:', err);
    }
}

async function fetchMessages(queueName) {
    try {
        const msgs = await fetchJSON('/api/messages?queue=' + encodeURIComponent(queueName));
        renderMessages(msgs || []);
    } catch(err) {
        console.error('Fetch messages error:', err);
        document.getElementById('modal-body').innerHTML =
            '<div class="empty-state"><span>❌</span>Error loading messages</div>';
    }
}

// ─── ACK message ────────────────────────────────────────────
async function ackMessage(queueName, messageId, btn) {
    btn.disabled = true;
    btn.textContent = '…';
    try {
        const res = await fetch('/api/ack', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ queue_name: queueName, message_id: messageId })
        });
        const data = await res.json();
        if (res.ok) {
            showToast('Message ACKed successfully', 'success');
            // Refresh message list
            fetchMessages(queueName);
            // Also refresh main data
            fetchAll();
        } else {
            showToast(data.error || 'ACK failed', 'error');
            btn.disabled = false;
            btn.textContent = 'ACK';
        }
    } catch(err) {
        showToast('Network error', 'error');
        btn.disabled = false;
        btn.textContent = 'ACK';
    }
}

// ─── Toast ──────────────────────────────────────────────────
function showToast(msg, type) {
    const el = document.createElement('div');
    el.className = 'toast toast-' + type;
    el.textContent = msg;
    document.body.appendChild(el);
    setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity .3s'; }, 2000);
    setTimeout(() => el.remove(), 2400);
}

// ─── Render: Overview ───────────────────────────────────────
function renderOverview(o) {
    animateValue('st-queues',    o.total_queues);
    animateValue('st-exchanges', o.total_exchanges);
    animateValue('st-consumers', o.total_consumers);
    animateValue('st-messages',  o.total_messages);
}

function animateValue(id, target) {
    const el = document.getElementById(id);
    const current = parseInt(el.textContent) || 0;
    if (current === target) { el.textContent = target; return; }
    const duration = 400;
    const start = performance.now();
    function step(ts) {
        const p = Math.min((ts - start) / duration, 1);
        el.textContent = Math.round(current + (target - current) * p);
        if (p < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
}

// ─── Render: Queues ─────────────────────────────────────────
function renderQueues(list) {
    document.getElementById('badge-queues').textContent = list.length;
    if (!list.length) {
        document.getElementById('table-queues').innerHTML =
            '<div class="empty-state"><span>📦</span>No queues declared yet</div>';
        return;
    }
    let html = '<table><thead><tr>' +
        '<th>Name</th><th>Messages</th><th>Capacity</th><th>Usage</th><th>In-Flight</th><th>Consumers</th><th></th>' +
        '</tr></thead><tbody>';
    list.forEach(q => {
        const pct = q.capacity > 0 ? Math.round(q.message_count / q.capacity * 100) : 0;
        html += '<tr>' +
            '<td><span class="mono queue-link" onclick="openModal(\'' + escAttr(q.name) + '\')">' + esc(q.name) + '</span></td>' +
            '<td>' + q.message_count + '</td>' +
            '<td>' + q.capacity + '</td>' +
            '<td>' + pct + '% <div class="progress-bar-bg"><div class="progress-bar-fill" style="width:' + pct + '%"></div></div></td>' +
            '<td>' + q.inflight_count + '</td>' +
            '<td>' + q.consumer_count + '</td>' +
            '<td><button class="btn-ack" style="background:var(--gradient-cyan);font-size:.7rem;padding:3px 10px" onclick="openModal(\'' + escAttr(q.name) + '\')">View</button></td>' +
            '</tr>';
    });
    html += '</tbody></table>';
    document.getElementById('table-queues').innerHTML = html;
}

// ─── Render: Exchanges ──────────────────────────────────────
function renderExchanges(list) {
    document.getElementById('badge-exchanges').textContent = list.length;
    if (!list.length) {
        document.getElementById('table-exchanges').innerHTML =
            '<div class="empty-state"><span>🔀</span>No exchanges declared</div>';
        return;
    }
    let html = '<table><thead><tr>' +
        '<th>Name</th><th>Type</th><th>Bindings</th>' +
        '</tr></thead><tbody>';
    list.forEach(ex => {
        const name = ex.name === '' ? '<em>(default)</em>' : esc(ex.name);
        const tag = 'tag-' + ex.type;
        const bindings = (ex.bindings || []).map(b =>
            '<span class="mono" style="font-size:.78rem">' + esc(b.routing_key || '*') + ' → ' + esc(b.queue_name) + '</span>'
        ).join('<br>') || '<span style="color:var(--text-muted)">none</span>';
        html += '<tr>' +
            '<td class="mono">' + name + '</td>' +
            '<td><span class="tag ' + tag + '">' + esc(ex.type) + '</span></td>' +
            '<td>' + bindings + '</td>' +
            '</tr>';
    });
    html += '</tbody></table>';
    document.getElementById('table-exchanges').innerHTML = html;
}

// ─── Render: Consumers ─────────────────────────────────────
function renderConsumers(list) {
    document.getElementById('badge-consumers').textContent = list.length;
    if (!list.length) {
        document.getElementById('table-consumers').innerHTML =
            '<div class="empty-state"><span>🎧</span>No active consumers</div>';
        return;
    }
    let html = '<table><thead><tr>' +
        '<th>Consumer Tag</th><th>Queue</th><th>Channel</th><th>Auto-Ack</th>' +
        '</tr></thead><tbody>';
    list.forEach(c => {
        const ack = c.auto_ack;
        html += '<tr>' +
            '<td class="mono">' + esc(c.consumer_tag) + '</td>' +
            '<td class="mono">' + esc(c.queue_name) + '</td>' +
            '<td>' + c.channel_id + '</td>' +
            '<td><span class="tag tag-' + ack + '">' + (ack ? 'yes' : 'no') + '</span></td>' +
            '</tr>';
    });
    html += '</tbody></table>';
    document.getElementById('table-consumers').innerHTML = html;
}

// ─── Render: Messages (modal) ──────────────────────────────
function renderMessages(list) {
    const body = document.getElementById('modal-body');
    if (!list.length) {
        body.innerHTML = '<div class="empty-state"><span>📭</span>No messages in this queue</div>';
        return;
    }
    let html = '<table><thead><tr>' +
        '<th>Status</th><th>Message ID</th><th>Issuer</th><th>Routing</th><th>Timestamp</th><th>Payload</th><th>Action</th>' +
        '</tr></thead><tbody>';
    list.forEach(m => {
        const statusTag = m.status === 'inflight' ? 'tag-inflight' : 'tag-queued';
        const payloadStr = formatPayload(m.payload);
        const ackBtn = '<button class="btn-ack" onclick="ackMessage(\'' + escAttr(currentModalQueue) + '\',\'' + escAttr(m.message_id) + '\', this)">ACK</button>';

        html += '<tr>' +
            '<td><span class="tag ' + statusTag + '">' + esc(m.status) + '</span></td>' +
            '<td class="mono" style="font-size:.72rem">' + esc(m.message_id) + '</td>' +
            '<td>' + esc(m.issuer) + '</td>' +
            '<td class="mono" style="font-size:.78rem">' + esc(m.routing) + '</td>' +
            '<td style="font-size:.82rem;white-space:nowrap">' + esc(m.timestamp) + '</td>' +
            '<td><div class="payload-preview" onclick="showPayloadPopover(this, ' + escJsonAttr(payloadStr) + ')" title="Click to expand">' + esc(payloadStr) + '</div></td>' +
            '<td>' + ackBtn + '</td>' +
            '</tr>';
    });
    html += '</tbody></table>';
    body.innerHTML = html;
}

// ─── Payload Popover ────────────────────────────────────────
function showPayloadPopover(el, text) {
    const pop = document.getElementById('payload-popover');
    try {
        const obj = JSON.parse(text);
        pop.textContent = JSON.stringify(obj, null, 2);
    } catch(e) {
        pop.textContent = text;
    }
    const rect = el.getBoundingClientRect();
    pop.style.top = (rect.bottom + 8) + 'px';
    pop.style.left = Math.min(rect.left, window.innerWidth - 520) + 'px';
    pop.style.display = 'block';

    // Close on outside click
    setTimeout(() => {
        document.addEventListener('click', hidePayloadPopover, {once:true});
    }, 10);
}
function hidePayloadPopover() {
    document.getElementById('payload-popover').style.display = 'none';
}

// ─── Helpers ────────────────────────────────────────────────
function esc(s) {
    if (s === null || s === undefined) return '';
    const d = document.createElement('div');
    d.textContent = String(s);
    return d.innerHTML;
}
function escAttr(s) {
    return String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
}
function escJsonAttr(s) {
    // Encode for use as a JS string argument inside onclick
    return "'" + String(s).replace(/\\/g,'\\\\').replace(/'/g,"\\'").replace(/</g,'\\x3c') + "'";
}
function formatPayload(p) {
    if (p === null || p === undefined) return '(empty)';
    if (typeof p === 'string') return p;
    try {
        return JSON.stringify(p);
    } catch(e) {
        return String(p);
    }
}

// ─── Init ───────────────────────────────────────────────────
fetchAll();
setInterval(() => {
    fetchAll();
    // If modal is open, refresh its messages too
    if (currentModalQueue) fetchMessages(currentModalQueue);
}, 3000);
</script>

</body>
</html>` + ""
