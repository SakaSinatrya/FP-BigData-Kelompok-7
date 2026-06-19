/**
 * Dashboard JS – Dampak Kurs USD terhadap Harga Pangan Indonesia
 */

const REFRESH_INTERVAL = 30000; // 30 detik

let kursChart    = null;
let monthlyChart = null;
let corrChart    = null;

function fmt(val) {
  return new Intl.NumberFormat('id-ID', {
    style: 'currency', currency: 'IDR', minimumFractionDigits: 0
  }).format(val);
}

function monthLabel(year, month) {
  const d = new Date(year, month - 1, 1);
  return d.toLocaleDateString('id-ID', { month: 'short', year: '2-digit' });
}

// ── Speed Layer: kurs live ─────────────────────────────────────────────────
async function loadKurs() {
  try {
    const res  = await fetch('/api/kurs');
    const data = await res.json();

    document.getElementById('kurs-current').textContent =
      data.current_rate ? fmt(data.current_rate) : '–';
    document.getElementById('kurs-date').textContent =
      data.history[0]?.date ? `Per ${data.history[0].date}` : '';

    const history = [...data.history].reverse();
    const labels  = history.map(d => d.date ? d.date.substring(5) : '');
    const rates   = history.map(d => d.rate);

    if (kursChart) kursChart.destroy();
    kursChart = new Chart(document.getElementById('kursChart').getContext('2d'), {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'Kurs USD-IDR',
          data: rates,
          borderColor: '#0d6efd',
          backgroundColor: 'rgba(13,110,253,.1)',
          borderWidth: 2,
          fill: true,
          tension: 0.3,
          pointRadius: 3
        }]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          y: { beginAtZero: false, title: { display: true, text: 'IDR' } }
        }
      }
    });

    const status = data.source === 'live' ? '🟢 Live data tersedia' : '🟡 Belum ada data live';
    document.getElementById('pipeline-status').textContent = status;
  } catch (e) {
    console.error('loadKurs:', e);
  }
}

// ── Speed Layer: pangan live ───────────────────────────────────────────────
async function loadPangan() {
  try {
    const res  = await fetch('/api/pangan');
    const data = await res.json();

    const container = document.getElementById('pangan-list');
    if (!data.items || data.items.length === 0) {
      container.innerHTML = '<p class="text-muted">Belum ada data live pangan. Jalankan producer & consumer terlebih dahulu.</p>';
      return;
    }

    container.innerHTML = data.items.map(item => `
      <div class="col-md-4 col-lg-3 mb-3">
        <div class="card border-success h-100">
          <div class="card-body text-center">
            <div class="fw-semibold">${item.name}</div>
            <div class="fs-5 text-success fw-bold mt-1">${fmt(item.price)}</div>
            <small class="text-muted">${item.date || ''}</small>
          </div>
        </div>
      </div>
    `).join('');
  } catch (e) {
    console.error('loadPangan:', e);
  }
}

// ── Batch Layer: tren bulanan dari Gold ────────────────────────────────────
async function loadMonthly() {
  try {
    const [kursRes, panganRes] = await Promise.all([
      fetch('/api/monthly_kurs'),
      fetch('/api/monthly_pangan')
    ]);
    const kursData   = await kursRes.json();
    const panganData = await panganRes.json();

    if (!kursData.length) {
      document.getElementById('monthlyChart').parentElement.innerHTML =
        '<p class="text-muted">Belum ada data gold. Jalankan spark/gold_layer.py terlebih dahulu.</p>';
      return;
    }

    const labels = kursData.map(d => monthLabel(d.year, d.month));
    const kursVals = kursData.map(d => d.avg_kurs);

    const COLORS = ['#dc3545','#fd7e14','#6f42c1','#20c997','#0dcaf0'];
    const komoList = Object.keys(panganData);

    const datasets = [
      {
        label: 'Kurs USD-IDR (IDR)',
        data: kursVals,
        borderColor: '#0d6efd',
        backgroundColor: 'rgba(13,110,253,.08)',
        borderWidth: 2,
        fill: false,
        yAxisID: 'y1',
        tension: 0.3
      },
      ...komoList.map((k, i) => {
        const hargaMap = {};
        (panganData[k] || []).forEach(d => {
          hargaMap[monthLabel(d.year, d.month)] = d.avg_harga;
        });
        return {
          label: k,
          data: labels.map(l => hargaMap[l] ?? null),
          borderColor: COLORS[i % COLORS.length],
          backgroundColor: 'transparent',
          borderWidth: 2,
          spanGaps: true,
          yAxisID: 'y2',
          tension: 0.3
        };
      })
    ];

    if (monthlyChart) monthlyChart.destroy();
    monthlyChart = new Chart(document.getElementById('monthlyChart').getContext('2d'), {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        interaction: { mode: 'index', intersect: false },
        plugins: { legend: { position: 'top' } },
        scales: {
          y1: {
            type: 'linear', position: 'left',
            title: { display: true, text: 'Kurs (IDR/USD)' },
            beginAtZero: false
          },
          y2: {
            type: 'linear', position: 'right',
            title: { display: true, text: 'Harga Pangan (IDR/kg)' },
            beginAtZero: false,
            grid: { drawOnChartArea: false }
          }
        }
      }
    });
  } catch (e) {
    console.error('loadMonthly:', e);
  }
}

// ── Batch Layer: FPVI, peringatan dini, proyeksi dampak kurs ───────────────
async function loadAnalytics() {
  try {
    const res  = await fetch('/api/analytics');
    const data = await res.json();
    const tbody = document.getElementById('analytics-table-body');

    if (data.error || !data.komoditas) {
      tbody.innerHTML = `<tr><td colspan="7" class="text-muted">${data.error || 'Belum ada data analitik. Jalankan spark/gold_layer.py terlebih dahulu.'}</td></tr>`;
      return;
    }

    document.getElementById('proyeksi-header').textContent = `Proyeksi (${data.horizon_minggu} minggu)`;
    document.getElementById('analytics-summary').innerHTML = `
      Kurs terakhir: <strong>${fmt(data.kurs_terakhir)}</strong> &nbsp;|&nbsp;
      Tren bulan terakhir: <strong class="${data.kurs_trend_per_bulan >= 0 ? 'text-danger' : 'text-success'}">
        ${data.kurs_trend_per_bulan >= 0 ? '+' : ''}${fmt(data.kurs_trend_per_bulan)}/bulan
      </strong> &nbsp;|&nbsp;
      Proyeksi kurs ${data.horizon_minggu} minggu ke depan: <strong>${fmt(data.kurs_proyeksi)}</strong>
    `;

    const levelBadge = (level) => {
      const cls = level === 'Tinggi' ? 'bg-danger' : level === 'Sedang' ? 'bg-warning text-dark' : 'bg-success';
      return `<span class="badge ${cls}">${level}</span>`;
    };
    const statusBadge = (status) => {
      const cls = status === 'RAWAN NAIK' ? 'bg-danger' : status === 'POTENSI TURUN' ? 'bg-primary' : 'bg-secondary';
      return `<span class="badge ${cls}">${status}</span>`;
    };

    tbody.innerHTML = data.komoditas.map((k, i) => `
      <tr style="cursor:pointer" onclick="document.getElementById('insight-row-${i}').classList.toggle('d-none')">
        <td class="fw-semibold">🔻 ${k.komoditas}</td>
        <td>${k.fpvi_score.toFixed(1)}</td>
        <td>${levelBadge(k.fpvi_level)}</td>
        <td>${statusBadge(k.status_peringatan)}</td>
        <td>${fmt(k.harga_terakhir)}</td>
        <td>${fmt(k.proyeksi_harga)}</td>
        <td class="${k.estimasi_dampak_persen >= 0 ? 'text-danger' : 'text-success'} fw-semibold">
          ${k.estimasi_dampak_persen >= 0 ? '+' : ''}${k.estimasi_dampak_persen}%
        </td>
      </tr>
      <tr id="insight-row-${i}" class="d-none">
        <td colspan="7" class="bg-light small">
          <strong>Mekanisme sebab-akibat:</strong> ${k.insight || 'Tidak ada penjelasan tersedia.'}
        </td>
      </tr>
    `).join('');
  } catch (e) {
    console.error('loadAnalytics:', e);
  }
}

// ── Batch Layer: korelasi per komoditas ────────────────────────────────────
async function loadCorrelation() {
  try {
    const res  = await fetch('/api/correlation');
    const data = await res.json();

    if (!data.by_komoditas || !data.by_komoditas.length) {
      document.getElementById('corrChart').parentElement.innerHTML =
        '<p class="text-muted">Belum ada data korelasi. Jalankan spark/gold_layer.py terlebih dahulu.</p>';
      return;
    }

    const sorted = [...data.by_komoditas].sort((a, b) => b.pearson_corr - a.pearson_corr);
    const labels   = sorted.map(d => d.komoditas);
    const valsKurs = sorted.map(d => d.pearson_corr);
    const valsOil  = sorted.map(d => d.pearson_corr_oil ?? null);

    if (corrChart) corrChart.destroy();
    corrChart = new Chart(document.getElementById('corrChart').getContext('2d'), {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label: 'vs Kurs USD-IDR',
            data: valsKurs,
            backgroundColor: '#0d6efd',
            borderRadius: 4
          },
          {
            label: 'vs Minyak Brent',
            data: valsOil,
            backgroundColor: '#212529',
            borderRadius: 4
          }
        ]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { display: true, position: 'top' },
          tooltip: {
            callbacks: {
              label: ctx => ` ${ctx.dataset.label}: r = ${ctx.parsed.y?.toFixed(4) ?? 'N/A'}`
            }
          }
        },
        scales: {
          y: {
            min: -1, max: 1,
            title: { display: true, text: 'Korelasi Pearson (r)' }
          }
        }
      }
    });
  } catch (e) {
    console.error('loadCorrelation:', e);
  }
}

// ── Init ───────────────────────────────────────────────────────────────────
async function refreshAll() {
  document.getElementById('last-updated').textContent =
    'Diperbarui: ' + new Date().toLocaleTimeString('id-ID');
  await Promise.all([loadKurs(), loadPangan(), loadMonthly(), loadCorrelation(), loadAnalytics()]);
}

document.addEventListener('DOMContentLoaded', () => {
  refreshAll();
  setInterval(refreshAll, REFRESH_INTERVAL);
});