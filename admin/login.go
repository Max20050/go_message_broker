package admin

const loginHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GoMQ – Login</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        *, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }

        :root {
            --bg-primary:    #0a0e17;
            --bg-card:       #1a2236;
            --border:        #2a3550;
            --text-primary:  #e2e8f0;
            --text-secondary:#94a3b8;
            --text-muted:    #64748b;
            --accent-blue:   #3b82f6;
            --accent-rose:   #f43f5e;
            --gradient-blue: linear-gradient(135deg, #3b82f6, #2563eb);
            --radius:        16px;
        }

        html, body {
            height:100%;
            font-family: 'Inter', -apple-system, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
        }

        /* ─── Background effects ─── */
        body::before {
            content:'';
            position:fixed; inset:0;
            background:
                radial-gradient(ellipse 700px 500px at 30% 20%, rgba(59,130,246,.09), transparent),
                radial-gradient(ellipse 500px 400px at 70% 75%, rgba(139,92,246,.07), transparent);
            pointer-events:none;
        }

        /* ─── Floating particles ─── */
        .particles {
            position:fixed; inset:0; pointer-events:none; overflow:hidden; z-index:0;
        }
        .particle {
            position:absolute;
            width:3px; height:3px;
            border-radius:50%;
            background:rgba(59,130,246,.3);
            animation:float linear infinite;
        }
        @keyframes float {
            0%   { transform:translateY(100vh) scale(0); opacity:0; }
            10%  { opacity:1; }
            90%  { opacity:1; }
            100% { transform:translateY(-10vh) scale(1); opacity:0; }
        }

        /* ─── Center layout ─── */
        .login-wrapper {
            min-height:100vh;
            display:flex; align-items:center; justify-content:center;
            padding:24px;
            position:relative; z-index:1;
        }

        /* ─── Card ─── */
        .login-card {
            background:var(--bg-card);
            border:1px solid var(--border);
            border-radius:var(--radius);
            box-shadow:0 8px 40px rgba(0,0,0,.45), 0 0 60px rgba(59,130,246,.06);
            width:100%; max-width:420px;
            padding:44px 40px 40px;
            animation:cardIn .5s ease;
        }
        @keyframes cardIn {
            from { opacity:0; transform:translateY(16px) scale(.97); }
            to   { opacity:1; transform:translateY(0)   scale(1); }
        }

        /* ─── Logo ─── */
        .login-logo {
            display:flex; align-items:center; justify-content:center; gap:14px;
            margin-bottom:32px;
        }
        .login-logo .icon {
            width:48px; height:48px; border-radius:14px;
            background:var(--gradient-blue);
            display:grid; place-items:center;
            font-size:1.2rem; font-weight:800; color:#fff;
            box-shadow:0 0 24px rgba(59,130,246,.35);
        }
        .login-logo .title {
            font-size:1.4rem; font-weight:700; letter-spacing:-.5px;
        }
        .login-logo .subtitle {
            font-size:.78rem; color:var(--text-muted); margin-top:2px;
        }

        /* ─── Form ─── */
        .form-group {
            margin-bottom:20px;
        }
        .form-label {
            display:block;
            font-size:.78rem; font-weight:600; color:var(--text-muted);
            text-transform:uppercase; letter-spacing:.8px;
            margin-bottom:8px;
        }
        .form-input {
            width:100%;
            padding:12px 16px;
            border:1px solid var(--border);
            border-radius:10px;
            background:rgba(0,0,0,.2);
            color:var(--text-primary);
            font-family:inherit; font-size:.92rem;
            outline:none;
            transition:border-color .2s, box-shadow .2s;
        }
        .form-input::placeholder { color:var(--text-muted); }
        .form-input:focus {
            border-color:var(--accent-blue);
            box-shadow:0 0 0 3px rgba(59,130,246,.15);
        }

        /* ─── Button ─── */
        .btn-login {
            width:100%;
            padding:13px 0;
            border:none; border-radius:10px;
            background:var(--gradient-blue);
            color:#fff;
            font-family:inherit; font-size:.92rem; font-weight:600;
            cursor:pointer;
            transition:transform .15s, box-shadow .15s;
            margin-top:8px;
        }
        .btn-login:hover {
            transform:translateY(-2px);
            box-shadow:0 6px 20px rgba(59,130,246,.35);
        }
        .btn-login:active { transform:translateY(0); }
        .btn-login:disabled {
            opacity:.6; cursor:not-allowed;
            transform:none; box-shadow:none;
        }

        /* ─── Error message ─── */
        .login-error {
            display:none;
            padding:10px 16px;
            border-radius:8px;
            background:rgba(244,63,94,.1);
            border:1px solid rgba(244,63,94,.25);
            color:var(--accent-rose);
            font-size:.84rem; font-weight:500;
            margin-bottom:18px;
            animation:shake .4s ease;
        }
        .login-error.visible { display:block; }
        @keyframes shake {
            0%,100% { transform:translateX(0); }
            20%     { transform:translateX(-6px); }
            40%     { transform:translateX(6px); }
            60%     { transform:translateX(-4px); }
            80%     { transform:translateX(4px); }
        }

        /* ─── Footer ─── */
        .login-footer {
            text-align:center;
            margin-top:24px;
            font-size:.76rem;
            color:var(--text-muted);
        }
    </style>
</head>
<body>

<!-- Floating particles -->
<div class="particles" id="particles"></div>

<div class="login-wrapper">
    <div class="login-card">
        <!-- Logo -->
        <div class="login-logo">
            <div class="icon">MQ</div>
            <div>
                <div class="title">GoMQ Admin</div>
                <div class="subtitle">Message Broker Management</div>
            </div>
        </div>

        <!-- Error -->
        <div class="login-error" id="login-error">Invalid credentials</div>

        <!-- Form -->
        <form id="login-form" onsubmit="return handleLogin(event)">
            <div class="form-group">
                <label class="form-label" for="username">Username</label>
                <input class="form-input" type="text" id="username" name="username"
                       placeholder="Enter your username" autocomplete="username" autofocus required>
            </div>
            <div class="form-group">
                <label class="form-label" for="password">Password</label>
                <input class="form-input" type="password" id="password" name="password"
                       placeholder="Enter your password" autocomplete="current-password" required>
            </div>
            <button type="submit" class="btn-login" id="btn-login">Sign In</button>
        </form>

        <div class="login-footer">
            Authenticate to access the broker admin panel
        </div>
    </div>
</div>

<script>
// ─── Particles ──────────────────────────────────────────────
(function createParticles() {
    const container = document.getElementById('particles');
    for (let i = 0; i < 30; i++) {
        const p = document.createElement('div');
        p.className = 'particle';
        p.style.left = Math.random() * 100 + '%';
        p.style.animationDuration = (6 + Math.random() * 10) + 's';
        p.style.animationDelay = (Math.random() * 8) + 's';
        p.style.width = p.style.height = (2 + Math.random() * 3) + 'px';
        container.appendChild(p);
    }
})();

// ─── Login handler ──────────────────────────────────────────
async function handleLogin(e) {
    e.preventDefault();
    const btn = document.getElementById('btn-login');
    const errorEl = document.getElementById('login-error');
    const username = document.getElementById('username').value.trim();
    const password = document.getElementById('password').value;

    errorEl.classList.remove('visible');
    btn.disabled = true;
    btn.textContent = 'Signing in…';

    try {
        const res = await fetch('/api/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password })
        });

        if (res.ok) {
            // Redirect to dashboard
            window.location.href = '/';
        } else {
            const data = await res.json();
            errorEl.textContent = data.error || 'Invalid credentials';
            errorEl.classList.add('visible');
            btn.disabled = false;
            btn.textContent = 'Sign In';
            // Shake the password field
            document.getElementById('password').value = '';
            document.getElementById('password').focus();
        }
    } catch(err) {
        errorEl.textContent = 'Network error – is the broker running?';
        errorEl.classList.add('visible');
        btn.disabled = false;
        btn.textContent = 'Sign In';
    }
    return false;
}

// ─── Enter key on password ──────────────────────────────────
document.getElementById('password').addEventListener('keydown', e => {
    if (e.key === 'Enter') document.getElementById('login-form').requestSubmit();
});
</script>

</body>
</html>`
