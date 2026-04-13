'use strict';
// script.js
// ─────────────────────────────────────────────────────────────────────────────
// Chart instances 
// ─────────────────────────────────────────────────────────────────────────────
const charts = {};
function destroyChart(id) { if (charts[id]) { charts[id].destroy(); delete charts[id]; } }

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────
const fmt  = v => new Intl.NumberFormat('en-SA').format(Math.round(Number(v||0)));
const fmtD = v => v != null ? Number(v).toLocaleString('en-SA',{minimumFractionDigits:1,maximumFractionDigits:1}) : '—';
const sar  = v => 'SAR ' + fmt(v);
const pct  = v => v != null ? Number(v).toFixed(1) + '%' : '—';
const esc  = v => v == null ? '' : String(v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const dash = v => (v == null || v === '') ? '—' : esc(v);

function statusPill(s) {
  const map = {
    'Fully Delivered':    'pill-delivered',
    'Partially Delivered':'pill-partial',
    'Shipped (Full)':     'pill-shipped-full',
    'Shipped (Partial)':  'pill-shipped-partial',
    'PO Created':         'pill-created',
    'Cancelled':          'pill-cancelled',
  };
  return `<span class="pill ${map[s]||'pill-created'}">${esc(s||'—')}</span>`;
}

function riskPill(tier) {
  const map = {'High Risk':'pill-risk-high','Medium Risk':'pill-risk-medium','Low Risk':'pill-risk-low'};
  return `<span class="pill ${map[tier]||'pill-created'}">${esc(tier)}</span>`;
}

function progressBar(pct_val) {
  const w = Math.min(100, Math.max(0, Number(pct_val||0)));
  const color = w >= 70 ? 'var(--brand-green)' : w >= 40 ? 'var(--brand-orange)' : 'var(--brand-red)';
  return `${pct(pct_val)}<span class="progress-bar-wrap">
    <span class="progress-bar-fill" style="width:${w}%;background:${color};"></span></span>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Toast notifications
// ─────────────────────────────────────────────────────────────────────────────
function toast(type, title, msg = '', duration = 5000) {
  const icons = { success:'✅', error:'❌', info:'ℹ️' };
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.innerHTML = `
    <span class="toast-icon">${icons[type]||'ℹ️'}</span>
    <div class="toast-body">
      <div class="toast-title">${esc(title)}</div>
      ${msg ? `<div class="toast-msg">${msg}</div>` : ''}
    </div>
    <button class="toast-close" onclick="this.parentElement.remove()">✕</button>`;
  document.getElementById('toastContainer').appendChild(el);
  if (duration > 0) {
    setTimeout(() => {
      el.classList.add('removing');
      setTimeout(() => el.remove(), 300);
    }, duration);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Fetch helper
// ─────────────────────────────────────────────────────────────────────────────
async function api(url, opts = {}) {
  const r = await fetch(url, { headers: {'Content-Type':'application/json'}, ...opts });
  const txt = await r.text();
  const data = txt ? JSON.parse(txt) : {};
  if (!r.ok) throw new Error(data.detail || data.message || `HTTP ${r.status}`);
  return data;
}

// ─────────────────────────────────────────────────────────────────────────────
// Dark mode
// ─────────────────────────────────────────────────────────────────────────────
function initDark() {
  const saved = localStorage.getItem('prodash-theme') || 'light';
  document.documentElement.setAttribute('data-theme', saved);
}
document.getElementById('darkToggle').addEventListener('click', () => {
  const cur = document.documentElement.getAttribute('data-theme');
  const next = cur === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('prodash-theme', next);
  if (currentView === 'overview') loadOverview();
});
initDark();

// ─────────────────────────────────────────────────────────────────────────────
// Status pills
// ─────────────────────────────────────────────────────────────────────────────
async function loadStatus() {
  const [st, rd] = await Promise.all([api('/api/status'), api('/api/ready')]);
  document.getElementById('serverTime').textContent = st.server_time || '—';
  const pillApi   = document.getElementById('pillApi');
  const pillReady = document.getElementById('pillReady');
  const pillSync  = document.getElementById('pillSync');
  pillApi.textContent  = `API: ${st.status || 'unknown'}`;
  pillApi.className    = `status-pill ${st.status === 'online' ? 'online' : 'error'}`;
  pillReady.textContent = `Ready: ${rd.ready ? 'Yes' : 'No'}`;
  pillReady.className   = `status-pill ${rd.ready ? 'ready' : 'warning'}`;
  pillSync.textContent  = `Last sync: ${st.last_sync_time || 'never'}`;
  pillSync.className    = `status-pill ${st.last_sync_status === 'success' ? 'online' : 'warning'}`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Overview
// ─────────────────────────────────────────────────────────────────────────────
async function loadOverview() {
  await loadStatus();
  const d = await api('/api/analytics/dashboard');
  if (d.status !== 'success') { toast('info', 'Dashboard not ready', d.message || 'Run a sync first.'); return; }
  const k = d.kpis || {};

  document.getElementById('kpi-pos').textContent      = fmt(k.total_pos);
  document.getElementById('kpi-lines').textContent    = fmt(k.total_lines);
  document.getElementById('kpi-open-lines').textContent = `📂 ${fmt(k.open_lines)} open`;
  document.getElementById('kpi-spend').textContent    = sar(k.total_estimated_spend);
  document.getElementById('kpi-rate').textContent     = pct(k.fulfillment_rate_pct);
  document.getElementById('kpi-fulfilled-lines').textContent = `✓ ${fmt(k.fully_delivered_lines)} lines`;
  document.getElementById('kpi-suppliers').textContent = fmt(k.active_suppliers);
  document.getElementById('kpi-plants').textContent   = `🏗 ${fmt(k.active_plants)} plant(s)`;
  document.getElementById('kpi-s-delivered').textContent       = fmt(k.fully_delivered_lines);
  document.getElementById('kpi-s-partial').textContent         = fmt(k.partially_delivered_lines);
  document.getElementById('kpi-s-shipped-full').textContent    = fmt(k.shipped_full_lines);
  document.getElementById('kpi-s-shipped-partial').textContent = fmt(k.shipped_partial_lines);
  document.getElementById('kpi-s-open').textContent            = fmt(k.open_lines);
  document.getElementById('kpi-s-cancelled').textContent       = fmt(k.cancelled_lines);

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const gridColor = isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.05)';
  const tickColor = isDark ? '#8B95A8' : '#667085';

  const c = d.charts || {};

  // Spend timeline — gradient bar
  destroyChart('spend');
  const spendCtx = document.getElementById('spendChart').getContext('2d');
  const grad = spendCtx.createLinearGradient(0, 0, 0, 300);
  grad.addColorStop(0, 'rgba(242,140,27,0.85)');
  grad.addColorStop(1, 'rgba(242,140,27,0.3)');
  charts['spend'] = new Chart(spendCtx, {
    type: 'bar',
    data: {
      labels: (c.spend_timeline?.labels||[]).slice().reverse(),
      datasets: [{
        label: 'Spend (SAR)',
        data:  (c.spend_timeline?.data||[]).slice().reverse(),
        backgroundColor: grad,
        borderRadius: 6,
        borderSkipped: false,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { color: gridColor }, ticks: { color: tickColor, maxRotation: 45 } },
        y: { beginAtZero: true, grid: { color: gridColor },
             ticks: { color: tickColor, callback: v => 'SAR ' + fmt(v) } }
      }
    }
  });
  document.getElementById('spendBadge').textContent =
    (c.spend_timeline?.labels||[]).length + ' months';

  // Status doughnut
  destroyChart('status');
  charts['status'] = new Chart(document.getElementById('statusChart'), {
    type: 'doughnut',
    data: {
      labels: c.status_distribution?.labels || [],
      datasets: [{
        data: c.status_distribution?.data || [],
        backgroundColor: ['#059669','#15803D','#F28C1B','#B45309','#2563EB','#94A3B8'],
        borderWidth: 3,
        borderColor: isDark ? '#1E2233' : '#fff',
        hoverOffset: 6,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      cutout: '68%',
      plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 10, padding: 12,
          font: { size: 11 }, color: tickColor } }
      }
    }
  });

  // Top suppliers — horizontal bar
  destroyChart('suppliersChart');
  charts['suppliersChart'] = new Chart(document.getElementById('suppliersChart'), {
    type: 'bar',
    data: {
      labels: c.top_suppliers?.labels || [],
      datasets: [{
        label: 'Spend (SAR)',
        data:  c.top_suppliers?.data  || [],
        backgroundColor: 'rgba(37,99,235,0.75)',
        borderRadius: 5,
        borderSkipped: false,
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { beginAtZero: true, grid: { color: gridColor },
             ticks: { color: tickColor, callback: v => fmt(v) } },
        y: { grid: { display: false }, ticks: { color: tickColor, font: { size: 10 } } }
      }
    }
  });

  // Material group — doughnut
  destroyChart('matGroup');
  charts['matGroup'] = new Chart(document.getElementById('matGroupChart'), {
    type: 'doughnut',
    data: {
      labels: c.spend_by_material_group?.labels || [],
      datasets: [{
        data: c.spend_by_material_group?.data || [],
        backgroundColor: ['#F28C1B','#2563EB','#059669','#7C3AED','#D32F2F',
                          '#0891B2','#B45309','#15803D','#1D4ED8','#9D174D'],
        borderWidth: 3,
        borderColor: isDark ? '#1E2233' : '#fff',
        hoverOffset: 6,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      cutout: '55%',
      plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 10, padding: 10,
          font: { size: 10 }, color: tickColor } }
      }
    }
  });

  // Risk bar
  destroyChart('risk');
  const riskColors = (c.supplier_risk?.risk_tier||[]).map(t =>
    t === 'High Risk' ? '#D32F2F' : t === 'Medium Risk' ? '#F28C1B' : '#059669'
  );
  charts['risk'] = new Chart(document.getElementById('riskChart'), {
    type: 'bar',
    data: {
      labels: c.supplier_risk?.labels || [],
      datasets: [{
        label: 'Risk Score',
        data:  c.supplier_risk?.data  || [],
        backgroundColor: riskColors.length ? riskColors : '#F28C1B',
        borderRadius: 5,
        borderSkipped: false,
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { beginAtZero: true, grid: { color: gridColor }, ticks: { color: tickColor } },
        y: { grid: { display: false }, ticks: { color: tickColor, font: { size: 10 } } }
      }
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Purchase Orders
// ─────────────────────────────────────────────────────────────────────────────
let _ordersCache = [];

async function loadOrders() {
  const params = new URLSearchParams();
  const g = id => document.getElementById(id)?.value.trim() || '';
  
  if (g('f-search'))   params.set('search',        g('f-search'));
  if (g('f-status'))   params.set('status',         g('f-status'));
  if (g('f-supplier')) params.set('supplier',       g('f-supplier'));
  if (g('f-material')) params.set('material',       g('f-material'));
  if (g('f-po'))       params.set('po',             g('f-po'));
  if (g('f-pr'))       params.set('customer_pr',    g('f-pr'));
  if (g('f-matgroup')) params.set('material_group', g('f-matgroup'));
  if (g('f-plant'))    params.set('plant',          g('f-plant'));
  if (g('f-from'))     params.set('date_from',      g('f-from'));
  if (g('f-to'))       params.set('date_to',        g('f-to'));

  // NEW: Check the 24h filter checkbox
  const last24h = document.getElementById('f-24h')?.checked;
  if (last24h) {
    params.set('received_24h', 'true');
  }

  params.set('limit', '500');
  
  try {
    const d = await api(`/api/dashboard/dataset?${params}`);
    _ordersCache = d.data || [];
    renderOrders(_ordersCache);
  } catch (e) {
    toast('error', 'Failed to load orders', e.message);
  }
}
function renderOrders(rows) {
  document.getElementById('ordersCount').textContent = `${rows.length} records`;
  document.getElementById('ordersBody').innerHTML = rows.length
    ? rows.map(r => `
      <tr onclick="openDetailDrawer('orders', '${r.purchase_order}')">
        <td><strong>${dash(r.purchase_order)}</strong></td>
        <td>${dash(r.customer_po)}</td>
        <td>${dash(r.creation_date)}</td>
        <td>${statusPill(r.dashboard_status)}</td>
      </tr>`).join('')
    : `<tr><td colspan="4" style="text-align:center;padding:32px;color:var(--text-muted);">No records match.</td></tr>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Suppliers
// ─────────────────────────────────────────────────────────────────────────────
let _suppliersCache = [];

async function loadSuppliers() {
  const d = await api('/api/analytics/suppliers?limit=200');
  _suppliersCache = d.data || [];
  renderSuppliers(_suppliersCache);
}

function renderSuppliers(rows) {
  document.getElementById('suppliersCount').textContent = `${rows.length} suppliers`;
  document.getElementById('suppliersBody').innerHTML = rows.length
    ? rows.map((r, i) => `
      <tr onclick="openDetailDrawer('suppliers', '${r.supplier}')">
        <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;" title="${esc(r.supplier_name)}">${dash(r.supplier_name)}</td>
        <td><strong>${dash(r.supplier)}</strong></td>
        <td style="text-align:right;font-weight:600;">${sar(r.total_spend)}</td>
        <td>${progressBar(r.lines > 0 ? (r.delivered_lines / r.lines * 100) : 0)}</td>
      </tr>`).join('')
    : `<tr><td colspan="4" style="text-align:center;padding:32px;color:var(--text-muted);">No data.</td></tr>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Supplier Risk
// ─────────────────────────────────────────────────────────────────────────────
let _riskCache = [];

async function loadRisk() {
  const d = await api('/api/analytics/supplier-risk?limit=200');
  _riskCache = d.data || [];
  renderRisk(_riskCache);
}

function renderRisk(rows) {
  document.getElementById('riskCount').textContent = `${rows.length} suppliers`;
  document.getElementById('riskBody').innerHTML = rows.length
    ? rows.map((r, i) => `
      <tr onclick="openDetailDrawer('risk', '${r.supplier}')" class="risk-${(r.risk_tier||'').toLowerCase().replace(' risk','').replace(' ','-')}">
        <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;" title="${esc(r.supplier_name)}">${dash(r.supplier_name)}</td>
        <td><strong>${dash(r.supplier)}</strong></td>
        <td style="text-align:right;font-weight:700;">${fmt(r.risk_score)}</td>
        <td>${riskPill(r.risk_tier)}</td>
      </tr>`).join('')
    : `<tr><td colspan="4" style="text-align:center;padding:32px;color:var(--text-muted);">No data.</td></tr>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Overdue
// ─────────────────────────────────────────────────────────────────────────────
let _overdueCache = [];

async function loadOverdue() {
  const d = await api('/api/analytics/overdue?limit=500');
  _overdueCache = d.data || [];
  // Update sidebar badge
  const badge = document.getElementById('overdueBadge');
  if (_overdueCache.length > 0) {
    badge.textContent = _overdueCache.length > 99 ? '99+' : _overdueCache.length;
    badge.style.display = '';
  } else {
    badge.style.display = 'none';
  }
  renderOverdue(_overdueCache);
}

function renderOverdue(rows) {
  document.getElementById('overdueCount').textContent = `${rows.length} overdue lines`;
  document.getElementById('overdueBody').innerHTML = rows.length
    ? rows.map(r => {
        const overdueDays = (r.age_days||0) - (r.planned_delivery_days||30);
        return `
          <tr onclick="openDetailDrawer('orders', '${r.purchase_order}')">
          <td><strong>${dash(r.purchase_order)}</strong></td>
          <td>${dash(r.customer_po)}</td>
          <td style="text-align:right;">${fmt(r.age_days)}</td>
          <td style="text-align:right;color:var(--brand-red);font-weight:700;">+${fmt(overdueDays)}</td>
          <td>${statusPill(r.dashboard_status)}</td>
        </tr>`;
      }).join('')
    : `<tr><td colspan="5" style="text-align:center;padding:32px;color:var(--text-muted);">No overdue lines.</td></tr>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Delivery
// ─────────────────────────────────────────────────────────────────────────────
let _deliveryCache = [];

async function loadDelivery() {
  const d = await api('/api/analytics/delivery-performance?limit=200');
  _deliveryCache = d.data || [];
  renderDelivery(_deliveryCache);
}

function renderDelivery(rows) {
  document.getElementById('deliveryCount').textContent = `${rows.length} suppliers`;
  document.getElementById('deliveryBody').innerHTML = rows.length
    ? rows.map((r, i) => {
        return `
          <tr onclick="openDetailDrawer('risk', '${r.supplier}')">
          <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;" title="${esc(r.supplier_name)}">${dash(r.supplier_name)}</td>
          <td><strong>${dash(r.supplier)}</strong></td>
          <td>${progressBar(r.delivery_rate_pct)}</td>
          <td style="text-align:right;">${fmtD(r.avg_delay_days)}</td>
        </tr>`;
      }).join('')
    : `<tr><td colspan="4" style="text-align:center;padding:32px;color:var(--text-muted);">No data.</td></tr>`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Global search — live search against dataset API
// ─────────────────────────────────────────────────────────────────────────────
let _searchTimer = null;
let _searchCache = [];

const globalSearchEl = document.getElementById('globalSearch');
const searchDropdown = document.getElementById('searchDropdown');
const globalSearchClear = document.getElementById('globalSearchClear');

globalSearchEl.addEventListener('input', () => {
  const q = globalSearchEl.value.trim();
  if (!q) { searchDropdown.classList.remove('visible'); return; }
  clearTimeout(_searchTimer);
  _searchTimer = setTimeout(() => runGlobalSearch(q), 280);
});

globalSearchEl.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    globalSearchEl.value = '';
    searchDropdown.classList.remove('visible');
  }
});

globalSearchClear.addEventListener('click', () => {
  globalSearchEl.value = '';
  searchDropdown.classList.remove('visible');
});

document.addEventListener('click', e => {
  if (!document.getElementById('globalSearchWrap').contains(e.target)) {
    searchDropdown.classList.remove('visible');
  }
});

async function runGlobalSearch(q) {
  try {
    const d = await api(`/api/dashboard/dataset?search=${encodeURIComponent(q)}&limit=12`);
    const rows = d.data || [];
    _searchCache = rows;

    if (!rows.length) {
      searchDropdown.innerHTML = `<div class="search-no-results">No results for "<strong>${esc(q)}</strong>"</div>`;
      searchDropdown.classList.add('visible');
      return;
    }

    searchDropdown.innerHTML = rows.map((r, i) => `
      <div class="search-result-item" data-idx="${i}">
        <span class="sri-icon po">📦</span>
        <span class="sri-label">${esc(r.purchase_order)} / ${esc(r.purchase_order_item)}
          <span style="font-weight:400;color:var(--text-muted);"> — ${esc(r.supplier_name||r.supplier||'')}</span>
        </span>
        <span class="sri-meta">${statusPill(r.dashboard_status)}</span>
      </div>`).join('');

    searchDropdown.classList.add('visible');

    searchDropdown.querySelectorAll('.search-result-item').forEach(el => {
      el.addEventListener('click', () => {
        const row = _searchCache[+el.dataset.idx];
        searchDropdown.classList.remove('visible');
        globalSearchEl.value = '';
        // Jump to orders view and pre-fill search
        document.getElementById('f-search').value = row.purchase_order;
        activateView('orders');
        loadOrders();
      });
    });

  } catch (e) {
    searchDropdown.innerHTML = `<div class="search-no-results">Search unavailable</div>`;
    searchDropdown.classList.add('visible');
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-table client-side search (instant filter on cached data)
// ─────────────────────────────────────────────────────────────────────────────
function setupClientSearch(inputId, filterFn, renderFn) {
  const el = document.getElementById(inputId);
  if (!el) return;
  el.addEventListener('input', () => {
    const q = el.value.trim().toLowerCase();
    filterFn(q, renderFn);
  });
}

setupClientSearch('sup-search', (q, render) => {
  const filtered = q ? _suppliersCache.filter(r =>
    (r.supplier||'').toLowerCase().includes(q) ||
    (r.supplier_name||'').toLowerCase().includes(q)
  ) : _suppliersCache;
  renderSuppliers(filtered);
});

setupClientSearch('risk-search', (q, render) => {
  const tier = document.getElementById('risk-tier-filter')?.value || '';
  const filtered = _riskCache.filter(r =>
    (!q || (r.supplier||'').toLowerCase().includes(q) || (r.supplier_name||'').toLowerCase().includes(q)) &&
    (!tier || r.risk_tier === tier)
  );
  renderRisk(filtered);
});

document.getElementById('risk-tier-filter')?.addEventListener('change', () => {
  const q = document.getElementById('risk-search')?.value.trim().toLowerCase() || '';
  const tier = document.getElementById('risk-tier-filter').value;
  const filtered = _riskCache.filter(r =>
    (!q || (r.supplier||'').toLowerCase().includes(q) || (r.supplier_name||'').toLowerCase().includes(q)) &&
    (!tier || r.risk_tier === tier)
  );
  renderRisk(filtered);
});

setupClientSearch('overdue-search', (q) => {
  const filtered = q ? _overdueCache.filter(r =>
    (r.purchase_order||'').toLowerCase().includes(q) ||
    (r.supplier||'').toLowerCase().includes(q) ||
    (r.supplier_name||'').toLowerCase().includes(q) ||
    (r.material||'').toLowerCase().includes(q) ||
    (r.material_name||'').toLowerCase().includes(q)
  ) : _overdueCache;
  renderOverdue(filtered);
});

setupClientSearch('delivery-search', (q) => {
  const filtered = q ? _deliveryCache.filter(r =>
    (r.supplier||'').toLowerCase().includes(q) ||
    (r.supplier_name||'').toLowerCase().includes(q) ||
    (r.plant||'').toLowerCase().includes(q)
  ) : _deliveryCache;
  renderDelivery(filtered);
});

// ─────────────────────────────────────────────────────────────────────────────
// Filter panel toggle
// ─────────────────────────────────────────────────────────────────────────────
document.getElementById('filterToggleBtn').addEventListener('click', () => {
  const panel = document.getElementById('filterPanel');
  const chevron = document.getElementById('filterChevron');
  panel.classList.toggle('open');
  chevron.textContent = panel.classList.contains('open') ? '▴' : '▾';
});

// ─────────────────────────────────────────────────────────────────────────────
// Export to CSV
// ─────────────────────────────────────────────────────────────────────────────
function exportCSV(data, filename) {
  if (!data || !data.length) { toast('info', 'No data to export'); return; }
  const headers = Object.keys(data[0]);
  const csv = [
    headers.join(','),
    ...data.map(row => headers.map(h => {
      const v = row[h] == null ? '' : String(row[h]);
      return v.includes(',') || v.includes('"') || v.includes('\n')
        ? `"${v.replace(/"/g, '""')}"` : v;
    }).join(','))
  ].join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
  toast('success', 'Export complete', `${filename} downloaded.`, 3000);
}

document.getElementById('exportBtn').addEventListener('click', () => {
  const exportMap = {
    overview:  null,
    orders:    { data: _ordersCache,    name: 'prodash_orders.csv' },
    suppliers: { data: _suppliersCache, name: 'prodash_suppliers.csv' },
    risk:      { data: _riskCache,      name: 'prodash_risk.csv' },
    overdue:   { data: _overdueCache,   name: 'prodash_overdue.csv' },
    delivery:  { data: _deliveryCache,  name: 'prodash_delivery.csv' },
  };
  const target = exportMap[currentView];
  if (!target) { toast('info', 'Export not available for Overview charts'); return; }
  exportCSV(target.data, target.name);
});

// ─────────────────────────────────────────────────────────────────────────────
// Navigation
// ─────────────────────────────────────────────────────────────────────────────
const viewLoaders = {
  overview: loadOverview,
  orders:   loadOrders,
  suppliers:loadSuppliers,
  risk:     loadRisk,
  overdue:  loadOverdue,
  delivery: loadDelivery,
};

const pageTitles = {
  overview:  'Procurement Overview',
  orders:    'Purchase Orders',
  suppliers: 'Supplier Spend Analysis',
  risk:      'Supplier Risk',
  overdue:   'Overdue POs',
  delivery:  'Delivery Performance',
};

let currentView = 'overview';

async function activateView(id) {
  document.querySelectorAll('.view-section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('nav li').forEach(l => l.classList.remove('active'));
  document.getElementById(id)?.classList.add('active');
  document.querySelector(`nav li[data-view="${id}"]`)?.classList.add('active');
  document.getElementById('pageTitle').textContent = pageTitles[id] || 'ProDash';
  currentView = id;
  try {
    await viewLoaders[id]?.();
  } catch (e) {
    toast('error', 'Failed to load view', e.message || String(e));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Sync with animated steps
// ─────────────────────────────────────────────────────────────────────────────
function setSyncStep(active) {
  const steps = [
    document.getElementById('step1'),
    document.getElementById('step2'),
    document.getElementById('step3'),
  ];
  steps.forEach((s, i) => {
    s.classList.remove('active', 'done');
    if (i < active)      s.classList.add('done'),   s.innerHTML = s.innerHTML.replace(/^.*?(?=\s)/, '✅');
    else if (i === active) s.classList.add('active');
  });
}

async function runSync() {
  document.getElementById('syncOverlay').classList.add('show');
  document.getElementById('refreshBtn').disabled = true;
  document.getElementById('reloadBtn').disabled  = true;

  setSyncStep(0);
  const t0 = Date.now();

  // Animate steps roughly in line with actual time
  const stepTimer1 = setTimeout(() => setSyncStep(1), 3000);
  const stepTimer2 = setTimeout(() => setSyncStep(2), 7000);

  try {
    const r = await api('/api/sync', { method: 'POST' });
    clearTimeout(stepTimer1); clearTimeout(stepTimer2);
    if (r.status !== 'success') throw new Error('Sync failed');
    const secs = r.timing?.total_seconds || ((Date.now()-t0)/1000).toFixed(1);
    toast(
      'success',
      `Sync complete in ${secs}s`,
      `mart_dashboard_po_item: ${(r.output_tables?.mart_dashboard_po_item||0).toLocaleString()} rows`,
      7000
    );
    await activateView(currentView);
  } catch (e) {
    toast('error', 'Sync failed', e.message || String(e), 0);
  } finally {
    clearTimeout(stepTimer1); clearTimeout(stepTimer2);
    document.getElementById('syncOverlay').classList.remove('show');
    document.getElementById('refreshBtn').disabled = false;
    document.getElementById('reloadBtn').disabled  = false;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event listeners
// ─────────────────────────────────────────────────────────────────────────────
document.querySelectorAll('nav li').forEach(li =>
  li.addEventListener('click', () => activateView(li.dataset.view))
);
document.getElementById('refreshBtn').addEventListener('click', runSync);
document.getElementById('reloadBtn').addEventListener('click', async () => {
  try { await activateView(currentView); }
  catch (e) { toast('error', 'Reload failed', e.message); }
});
document.getElementById('ordersApplyBtn').addEventListener('click', async () => {
  try { await loadOrders(); } catch (e) { toast('error', 'Filter error', e.message); }
});
document.getElementById('ordersClearBtn').addEventListener('click', async () => {
  // 1. Clear all text and date inputs
  ['f-search','f-supplier','f-material','f-po','f-pr','f-matgroup','f-plant','f-from','f-to']
    .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });

  // 2. Reset the Status dropdown
  document.getElementById('f-status').value = '';

  // 3. NEW: Uncheck the Last 24 Hours checkbox
  const f24 = document.getElementById('f-24h');
  if (f24) f24.checked = false;

  // 4. UI Adjustments
  document.getElementById('filterPanel').classList.remove('open');
  document.getElementById('filterChevron').textContent = '▾';

  // 5. Reload data
  try { 
    await loadOrders(); 
  } catch (e) { 
    toast('error', 'Clear error', e.message); 
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// DETAIL DRAWER
// ─────────────────────────────────────────────────────────────────────────────
function openDetailDrawer(type, id) {
  let row = null;
  if (type === 'orders' || type === 'overdue') {
    row = _ordersCache.find(r => r.purchase_order === id);
  } else if (type === 'suppliers') {
    row = _suppliersCache.find(r => r.supplier === id);
  } else if (type === 'risk') {
    row = _riskCache.find(r => r.supplier === id);
  }

  if (!row) return;

  const drawer = document.getElementById('detailDrawer');
  const overlay = document.getElementById('drawerOverlay');
  const body = document.getElementById('drawerBody');
  const title = document.getElementById('drawerTitle');
  const subtitle = document.getElementById('drawerSubtitle');

  title.textContent = row.purchase_order ? `PO ${row.purchase_order} Details` : `Supplier ${row.supplier} Details`;
  subtitle.textContent = row.supplier_name || row.material_name || "Record Metadata";

  // Group fields
  const financials = ['total_spend', 'net_price', 'estimated_spend', 'document_currency'];
  const logistics = ['creation_date', 'earliest_asn_delivery_date', 'last_gr_date', 'planned_delivery_days', 'days_po_to_asn', 'age_days'];
  const master = ['purchase_order', 'purchase_order_item', 'customer_pr', 'supplier', 'supplier_name', 'material', 'material_name', 'material_group', 'plant', 'requisitioner', 'cost_center', 'wbs_element'];

  let html = '';

  // Strategic Section
  if (row.planned_delivery_days && row.age_days) {
    const gap = (row.age_days || 0) - (row.planned_delivery_days || 0);
    html += `
      <div class="drawer-section">
        <h3>Strategic Analysis</h3>
        <div class="analysis-card">
          <div class="analysis-title">⏱️ Lead Time Reality Gap</div>
          <div style="font-size: 24px; font-weight: 700; color: ${gap > 0 ? 'var(--brand-red)' : 'var(--brand-green)'};">
            ${gap > 0 ? '+' : ''}${gap} Days Variance
          </div>
          <div style="font-size: 12px; opacity: 0.8;">Planned: ${row.planned_delivery_days} vs Actual: ${row.age_days}</div>
        </div>
      </div>`;
  }

  const renderGroup = (label, fields) => {
    const validFields = fields.filter(f => row[f] != null && row[f] !== '');
    if (!validFields.length) return '';
    return `
      <div class="drawer-section">
        <h3>${label}</h3>
        <div class="detail-grid">
          ${validFields.map(f => `
            <div class="detail-item">
              <div class="detail-label">${f.replace(/_/g, ' ')}</div>
              <div class="detail-value">${f.includes('spend') ? sar(row[f]) : dash(row[f])}</div>
            </div>
          `).join('')}
        </div>
      </div>`;
  };

  html += renderGroup('Master Identification', master);
  html += renderGroup('Logistics & Timing', logistics);
  html += renderGroup('Financial Metadata', financials);

  body.innerHTML = html;
  drawer.classList.add('active');
  overlay.classList.add('active');
}

function closeDrawer() {
  document.getElementById('detailDrawer').classList.remove('active');
  document.getElementById('drawerOverlay').classList.remove('active');
}

// ─────────────────────────────────────────────────────────────────────────────
// Boot
// ─────────────────────────────────────────────────────────────────────────────
window.addEventListener('load', async () => {
  try { await activateView('overview'); }
  catch (e) { toast('error', 'Load failed', e.message); }
});