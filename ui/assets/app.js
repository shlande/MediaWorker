/* MediaWorker Admin — 共享交互层
   侧边导航注入、抽屉、二次确认弹窗、toast(最终一致语义)、tabs。 */
(function () {
  const NAV = [
    { group: '控制面' },
    { id: 'dashboard', href: 'dashboard.html', label: '总览仪表盘', icon: 'M3 13h4v8H3zM9 3h4v18H9zM15 8h4v13h-4z' },
    { id: 'nodes',     href: 'nodes.html',     label: '节点管理',     icon: 'M12 2l8 4.5v9L12 20l-8-4.5v-9L12 2zM12 11l8-4.5M12 11v9M12 11L4 6.5' },
    { id: 'accounts',  href: 'accounts.html',  label: '云盘账号',     icon: 'M4 17a4 4 0 010-8 5.5 5.5 0 0110.7-1.5A4.5 4.5 0 0119 16.5V17H4z' },
    { id: 'content',   href: 'content.html',   label: '内容与 Pin',   icon: 'M4 4h16v16H4zM4 9h16M9 9v11' },
    { group: '策略域' },
    { id: 'policy',    href: 'policy.html',    label: '策略与配额',   icon: 'M12 3v18M5 7l7-4 7 4M7 21h10M9 12h6' },
    { id: 'audit',     href: 'audit.html',     label: '审计日志',     icon: 'M8 3h8l4 4v14H8zM8 3v18M12 12l2 2 4-4' },
    { group: '节点本地 · Edge-Node' },
    { id: 'edge-node',    href: 'edge-node.html',    label: '节点概览与缓存', icon: 'M4 5h16v10H4zM2 19h20M9 15v4M15 15v4' },
    { id: 'edge-pins',    href: 'edge-pins.html',    label: 'Pin 管理',       icon: 'M9 4h6l-1 7 3 3v2H7v-2l3-3-1-7zM12 16v5' },
    { id: 'edge-network', href: 'edge-network.html', label: '网络与回源',     icon: 'M5 12a3 3 0 106 0 3 3 0 00-6 0zM15 5a3 3 0 106 0 3 3 0 00-6 0zM15 19a3 3 0 106 0 3 3 0 00-6 0zM10.7 10.8l3-2.6M10.7 13.2l3 2.6' },
  ];

  const ICON = (d) =>
    `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round"><path d="${d}"/></svg>`;

  function injectSidebar(active) {
    const el = document.querySelector('.sidebar');
    if (!el) return;
    let html = `
      <div class="brand">
        <div class="mark">MW</div>
        <div>
          <div class="name">MediaWorker</div>
          <div class="sub">CONTROL&nbsp;PLANE</div>
        </div>
      </div>`;
    for (const item of NAV) {
      if (item.group) { html += `<div class="nav-label">${item.group}</div>`; continue; }
      html += `<a href="${item.href}" class="${item.id === active ? 'active' : ''}">${ICON(item.icon)}${item.label}</a>`;
    }
    html += `
      <div class="foot">
        <span>admin@mediaworker.internal</span>
        <span>角色 · 平台管理员</span>
        <a href="login.html" style="color:inherit;text-decoration:underline;opacity:.75;">切换账号 / 重新登录</a>
      </div>`;
    el.innerHTML = html;
  }

  function injectChrome() {
    const scrim = document.createElement('div');
    scrim.className = 'scrim';
    scrim.id = 'scrim';
    scrim.addEventListener('click', closeAll);
    document.body.appendChild(scrim);
    const toasts = document.createElement('div');
    toasts.id = 'toasts';
    document.body.appendChild(toasts);
    const btn = document.querySelector('.menu-btn');
    if (btn) btn.addEventListener('click', () => {
      document.querySelector('.sidebar').classList.toggle('open');
    });
  }

  /* ---------- 抽屉 ---------- */
  function openDrawer(id) {
    document.getElementById(id).classList.add('open');
    document.getElementById('scrim').classList.add('open');
  }
  function closeAll() {
    document.querySelectorAll('.drawer.open, .modal.open').forEach(e => e.classList.remove('open'));
    document.getElementById('scrim').classList.remove('open');
    document.querySelector('.sidebar')?.classList.remove('open');
  }

  /* ---------- Toast ---------- */
  function toast(msg, sub) {
    const t = document.createElement('div');
    t.className = 'toast';
    t.innerHTML = `<div>${msg}${sub ? `<span class="t-sub">${sub}</span>` : ''}</div>`;
    document.getElementById('toasts').appendChild(t);
    setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity .3s'; }, 4200);
    setTimeout(() => t.remove(), 4600);
  }
  /* 最终一致语义:管理操作下发后不是即时生效 */
  function dispatched(action) {
    toast(`${action} — 已下发,待生效`, '经控制通道传播,预计 6–10s 后全网一致');
  }

  /* ---------- 二次确认 ---------- */
  function confirmDanger({ title, body, phrase, actionLabel, onConfirm }) {
    let m = document.getElementById('danger-modal');
    if (!m) {
      m = document.createElement('div');
      m.className = 'modal';
      m.id = 'danger-modal';
      document.body.appendChild(m);
    }
    m.innerHTML = `
      <h3>${title}</h3>
      <p>${body}</p>
      <div class="confirm-phrase field">
        <label>输入 <code>${phrase}</code> 以确认</label>
        <input class="input mono" id="danger-input" placeholder="${phrase}" autocomplete="off" />
      </div>
      <div class="m-acts">
        <button class="btn" id="danger-cancel">取消</button>
        <button class="btn danger" id="danger-ok" disabled>${actionLabel}</button>
      </div>`;
    m.classList.add('open');
    document.getElementById('scrim').classList.add('open');
    const input = m.querySelector('#danger-input');
    const ok = m.querySelector('#danger-ok');
    input.focus();
    input.addEventListener('input', () => { ok.disabled = input.value.trim() !== phrase; });
    m.querySelector('#danger-cancel').onclick = closeAll;
    ok.onclick = () => { closeAll(); onConfirm && onConfirm(); };
  }

  /* ---------- Tabs ---------- */
  function tabs(rootSel) {
    document.querySelectorAll(rootSel).forEach(root => {
      const btns = root.querySelectorAll('.tabs button');
      btns.forEach(b => b.addEventListener('click', () => {
        btns.forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        root.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
        root.querySelector('#' + b.dataset.tab).classList.add('active');
      }));
    });
  }

  document.addEventListener('keydown', e => { if (e.key === 'Escape') closeAll(); });

  window.MW = { injectSidebar, injectChrome, openDrawer, closeAll, toast, dispatched, confirmDanger, tabs, ICON };
})();
