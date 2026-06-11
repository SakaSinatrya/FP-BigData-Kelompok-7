/**
 * Dashboard JavaScript
 * Fetch data dari API dan update dashboard
 */

let kursChart = null;

// Format currency
function formatCurrency(value) {
    return new Intl.NumberFormat('id-ID', {
        style: 'currency',
        currency: 'IDR',
        minimumFractionDigits: 0
    }).format(value);
}

// Load Kurs Data
async function loadKursData() {
    try {
        const response = await fetch('/api/kurs');
        const data = await response.json();
        
        document.getElementById('kurs-rate').innerHTML = `
            <h2 class="text-primary">${formatCurrency(data.current_rate)}</h2>
            <small class="text-muted">Diupdate: ${new Date(data.timestamp).toLocaleString('id-ID')}</small>
        `;
        
        // Update chart
        updateKursChart(data.history);
        
    } catch (error) {
        console.error('Error loading kurs data:', error);
        document.getElementById('kurs-rate').innerHTML = '<p class="text-danger">Error loading data</p>';
    }
}

// Load Pangan Data
async function loadPanganData() {
    try {
        const response = await fetch('/api/pangan');
        const data = await response.json();
        
        let html = '';
        data.items.forEach(item => {
            const changeClass = item.change.startsWith('+') ? 'change-positive' : 
                                item.change.startsWith('-') ? 'change-negative' : 
                                'change-neutral';
            
            html += `
                <div class="col-md-6 col-lg-3 mb-3">
                    <div class="pangan-item">
                        <div class="name">${item.name}</div>
                        <div class="price mt-2">${formatCurrency(item.price)}</div>
                        <div class="change ${changeClass} mt-2">${item.change}</div>
                    </div>
                </div>
            `;
        });
        
        document.getElementById('pangan-list').innerHTML = html;
        
    } catch (error) {
        console.error('Error loading pangan data:', error);
        document.getElementById('pangan-list').innerHTML = '<p class="text-danger">Error loading data</p>';
    }
}

// Load Correlation Data
async function loadCorrelationData() {
    try {
        const response = await fetch('/api/correlation');
        const data = await response.json();
        
        const correlationText = (data.correlation * 100).toFixed(1) + '%';
        document.getElementById('correlation-value').textContent = correlationText;
        
    } catch (error) {
        console.error('Error loading correlation data:', error);
        document.getElementById('correlation-value').textContent = 'Error';
    }
}

// Update Kurs Chart
function updateKursChart(historyData) {
    const ctx = document.getElementById('kursChart').getContext('2d');
    
    const dates = historyData.map(d => new Date(d.date).toLocaleDateString('id-ID', {month: 'short', day: 'numeric'}));
    const rates = historyData.map(d => d.rate);
    
    if (kursChart) {
        kursChart.destroy();
    }
    
    kursChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: dates,
            datasets: [{
                label: 'Kurs USD-IDR',
                data: rates,
                borderColor: '#0056b3',
                backgroundColor: 'rgba(0, 86, 179, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.3,
                pointRadius: 4,
                pointBackgroundColor: '#0056b3',
                pointBorderColor: '#fff',
                pointBorderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                }
            },
            scales: {
                y: {
                    beginAtZero: false,
                    title: {
                        display: true,
                        text: 'Kurs (IDR)'
                    }
                }
            }
        }
    });
}

// Refresh data every 30 seconds
function initializeRefresh() {
    loadKursData();
    loadPanganData();
    loadCorrelationData();
    
    setInterval(() => {
        loadKursData();
        loadPanganData();
        loadCorrelationData();
    }, 30000); // 30 seconds
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', initializeRefresh);
