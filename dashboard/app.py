"""
Flask Dashboard Application
Akses di http://localhost:5000
"""

from flask import Flask, render_template, jsonify
import json
from datetime import datetime, timedelta
import random

app = Flask(__name__)


@app.route('/')
def index():
    """Halaman utama dashboard"""
    return render_template('index.html')


@app.route('/api/kurs')
def get_kurs_data():
    """API endpoint untuk data kurs USD-IDR"""
    # TODO: Fetch dari database atau Spark
    data = {
        'current_rate': 15500.00,
        'timestamp': datetime.now().isoformat(),
        'history': [
            {'date': (datetime.now() - timedelta(days=i)).isoformat(), 
             'rate': 15500 + random.randint(-200, 200)}
            for i in range(7)
        ]
    }
    return jsonify(data)


@app.route('/api/pangan')
def get_pangan_data():
    """API endpoint untuk data harga pangan"""
    # TODO: Fetch dari database atau Spark
    data = {
        'items': [
            {'name': 'Beras Premium', 'price': 12000, 'change': '+2%'},
            {'name': 'Minyak Goreng', 'price': 18000, 'change': '-1%'},
            {'name': 'Daging Sapi', 'price': 95000, 'change': '+5%'},
            {'name': 'Telur Ayam', 'price': 28000, 'change': '0%'}
        ],
        'timestamp': datetime.now().isoformat()
    }
    return jsonify(data)


@app.route('/api/correlation')
def get_correlation():
    """API endpoint untuk korelasi kurs vs harga pangan"""
    # TODO: Fetch dari Spark Gold layer
    data = {
        'correlation': 0.65,
        'period': 'monthly',
        'timestamp': datetime.now().isoformat()
    }
    return jsonify(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
