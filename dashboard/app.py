"""
Flask Dashboard - Analisis Dampak Kurs USD terhadap Harga Pangan
Akses di http://localhost:8080
"""

import json
import os
from datetime import datetime
from flask import Flask, render_template, jsonify

app = Flask(__name__)

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
DATA_DIR      = os.path.join(BASE_DIR, "data")
GOLD_DIR      = os.path.join(DATA_DIR, "gold")
LIVE_KURS     = os.path.join(DATA_DIR, "live_kurs.json")
LIVE_PANGAN   = os.path.join(DATA_DIR, "live_pangan.json")


def _read_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/kurs")
def get_kurs():
    records = _read_json(LIVE_KURS, [])

    if records:
        latest  = records[0]
        current = latest.get("rate", 0)
        history = [
            {"date": r.get("timestamp", ""), "rate": r.get("rate", 0)}
            for r in records[:30]
        ]
    else:
        current = 0
        history = []

    return jsonify({
        "current_rate": current,
        "timestamp": datetime.now().isoformat(),
        "history": history,
        "source": "live" if records else "empty"
    })


@app.route("/api/pangan")
def get_pangan():
    records = _read_json(LIVE_PANGAN, [])

    latest_by_kom = {}
    for r in records:
        k = r.get("komoditas", "")
        if k and k not in latest_by_kom:
            latest_by_kom[k] = r

    items = [
        {
            "name":  k,
            "price": v.get("harga", 0),
            "date":  v.get("tanggal", "")
        }
        for k, v in latest_by_kom.items()
    ]

    return jsonify({
        "items": items,
        "timestamp": datetime.now().isoformat(),
        "source": "live" if records else "empty"
    })


@app.route("/api/monthly_kurs")
def get_monthly_kurs():
    data = _read_json(os.path.join(GOLD_DIR, "monthly_kurs.json"), [])
    return jsonify(data)


@app.route("/api/monthly_pangan")
def get_monthly_pangan():
    data = _read_json(os.path.join(GOLD_DIR, "monthly_pangan.json"), {})
    return jsonify(data)


@app.route("/api/correlation")
def get_correlation():
    data = _read_json(os.path.join(GOLD_DIR, "correlation.json"), [])
    if data:
        overall = round(sum(d["pearson_corr"] for d in data) / len(data), 4)
    else:
        overall = None

    return jsonify({
        "by_komoditas": data,
        "overall_avg": overall,
        "timestamp": datetime.now().isoformat(),
        "source": "gold" if data else "empty"
    })


@app.route("/api/analytics")
def get_analytics():
    data = _read_json(os.path.join(GOLD_DIR, "analytics.json"), {})
    return jsonify(data)


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)
    app.run(host="0.0.0.0", port=8080, debug=True)