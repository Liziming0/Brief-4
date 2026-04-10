// Init Map - Using Light CartoDB Tiles (minimal visual clutter)
const map = L.map('map', { zoomControl: false, attributionControl: false }).setView([51.5072, -0.1276], 11);
L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png').addTo(map);

// Core Config: Low Transparency (0.35) for better map legibility
const config = {
    'pm25': { color: 'rgba(255, 152, 0, 0.35)', grad: { 0.2: 'rgba(255,152,0,0)', 1.0: 'rgba(255,152,0,0.35)' } },
    'pm10': { color: 'rgba(244, 67, 54, 0.35)', grad: { 0.2: 'rgba(244,67,54,0)', 1.0: 'rgba(244,67,54,0.35)' } },
    'no2':  { color: 'rgba(156, 39, 176, 0.35)', grad: { 0.2: 'rgba(156,39,176,0)', 1.0: 'rgba(156,39,176,0.35)' } }
};

const districts = { 'City': [51.51, -0.12], 'West': [51.50, -0.30], 'East': [51.52, 0.05], 'North': [51.59, -0.11], 'South': [51.40, -0.12] };

let currentLayers = [];
let chart = null;
let sensorData = {};
let activeMode = 'pm25';

// 1. Fetch Real API Data (Open-Meteo)
async function refreshData() {
    try {
        const url = `https://air-quality-api.open-meteo.com/v1/air-quality?latitude=51.5072&longitude=-0.1276&current=pm10,pm2_5,nitrogen_dioxide`;
        const response = await fetch(url);
        const json = await response.json();
        const cur = json.current;

        // Sync API values with regional variations
        sensorData = {
            pm25: [cur.pm2_5, cur.pm2_5*1.2, cur.pm2_5*0.8, cur.pm2_5*0.9, cur.pm2_5*1.1],
            pm10: [cur.pm10, cur.pm10*1.1, cur.pm10*1.3, cur.pm10*0.7, cur.pm10*0.9],
            no2:  [cur.nitrogen_dioxide, cur.nitrogen_dioxide*1.4, cur.nitrogen_dioxide*0.9, cur.nitrogen_dioxide*0.8, cur.nitrogen_dioxide*1.2]
        };

        // Update Date/Time Display
        const now = new Date();
        document.getElementById('date-display').innerText = now.toLocaleString('en-GB', { 
            day: 'numeric', month: 'long', year: 'numeric', hour: '2-digit', minute: '2-digit' 
        });
        
        switchMode(activeMode);
    } catch (err) {
        console.error("API Fetch Error:", err);
    }
}

// 2. Generate Heatmap Coordinates
function generateHeat(dataArr) {
    let pts = [];
    let i = 0;
    for (let d in districts) {
        for(let n=0; n<75; n++) {
            pts.push([
                districts[d][0] + (Math.random()-0.5)*0.22, 
                districts[d][1] + (Math.random()-0.5)*0.22, 
                dataArr[i]
            ]);
        }
        i++;
    }
    return pts;
}

// 3. Switch Visualization Mode
function switchMode(mode) {
    activeMode = mode;
    currentLayers.forEach(l => map.removeLayer(l));
    currentLayers = [];

    // UI Button State
    document.querySelectorAll('button').forEach(b => b.classList.remove('active'));
    document.getElementById(`btn-${mode}`).classList.add('active');

    if (mode === 'overlay') {
        ['pm25', 'pm10', 'no2'].forEach(m => {
            const l = L.heatLayer(generateHeat(sensorData[m]), { radius: 60, blur: 50, max: 80, gradient: config[m].grad }).addTo(map);
            currentLayers.push(l);
        });
        updateChart('overlay');
    } else {
        const l = L.heatLayer(generateHeat(sensorData[mode]), { radius: 95, blur: 65, max: 100, gradient: config[mode].grad }).addTo(map);
        currentLayers.push(l);
        updateChart(mode);
    }
}

// 4. Update Analytical Chart
function updateChart(mode) {
    const ctx = document.getElementById('mainChart').getContext('2d');
    if (chart) chart.destroy();

    const chartConfig = {
        type: 'bar',
        options: { 
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { 
                x: { display: false }, 
                y: { grid: { display: false }, ticks: { font: { weight: 'bold', size: 10 } } } 
            }
        }
    };

    if (mode === 'overlay') {
        chartConfig.data = {
            labels: ['PM2.5', 'PM10', 'NO2'],
            datasets: [{
                data: [sensorData.pm25[0], sensorData.pm10[0], sensorData.no2[0]],
                backgroundColor: [config.pm25.color, config.pm10.color, config.no2.color],
                borderRadius: 6
            }]
        };
    } else {
        chartConfig.data = {
            labels: Object.keys(districts),
            datasets: [{
                data: sensorData[mode],
                backgroundColor: config[mode].color,
                borderRadius: 6
            }]
        };
    }
    chart = new Chart(ctx, chartConfig);
}

window.onload = () => {
    refreshData();
    // Hourly Sync
    setInterval(refreshData, 3000);
};