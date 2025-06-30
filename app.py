from flask import Flask, render_template, request, redirect, url_for, flash, session

import sqlite3, hashlib
from werkzeug.security import generate_password_hash, check_password_hash

import os
import requests
import gzip
import shutil
import datetime as dt
import numpy as np
import pandas as pd
from flask import jsonify





app = Flask(__name__)
app.secret_key = "dein_geheimes_schluessel"  # für Sessions, ändere das unbedingt

DATABASE = 'database.db'

def init_db():
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL
        );
    ''')
    conn.commit()
    conn.close()

def get_db_connection():
    conn = sqlite3.connect('database.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        confirm_password = request.form['confirm_password']

        # 1) Passwortgleichheit prüfen
        if password != confirm_password:
            flash('Die Passwörter stimmen nicht überein.')
            return redirect(url_for('register'))

        # 2) Existiert der Benutzer bereits?
        conn = get_db_connection()
        existing_user = conn.execute(
            'SELECT id FROM users WHERE username = ?', (username,)
        ).fetchone()
        if existing_user:
            conn.close()
            flash('Benutzername bereits vergeben.')
            return redirect(url_for('register'))

        # 3) Benutzer anlegen
        password_hash = generate_password_hash(password)
        conn.execute(
            'INSERT INTO users (username, password) VALUES (?, ?)',
            (username, password_hash)
        )
        conn.commit()
        conn.close()
        flash('Registrierung erfolgreich. Bitte logge dich ein.')
        return redirect(url_for('login'))

    return render_template('register.html')


NOAA_BASE_URL = "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes18/l1b/seis-l1b-sgps/"

def download_and_extract_goes18_data(start_date, end_date, download_dir="data"):
    os.makedirs(download_dir, exist_ok=True)
    current_date = start_date

    all_data = []

    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        url = f"{NOAA_BASE_URL}{current_date.strftime('%Y/%m/%d')}/"

        # Versuch, Verzeichnislisting abzurufen:
        try:
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Kein Verzeichnis für {date_str} gefunden (HTTP {response.status_code}).")
                current_date += dt.timedelta(days=1)
                continue

            # Suche alle SEIS-Dateien (Beispiel: sei_sgps_g18_dYYYYMMDD_tHHMMZ_cXXXX.nc.gz)
            filenames = [line.split('href="')[1].split('"')[0] for line in response.text.splitlines() if ".nc.gz" in line]

            for fname in filenames:
                file_url = url + fname
                local_path = os.path.join(download_dir, fname)
                print(f"Lade {file_url} herunter...")
                r = requests.get(file_url, stream=True)
                with open(local_path, 'wb') as f:
                    f.write(r.content)
                
                # Optional: entpacken, analysieren usw. Hier nur Demo:
                # with gzip.open(local_path, 'rb') as f_in:
                #     with open(local_path[:-3], 'wb') as f_out:
                #         shutil.copyfileobj(f_in, f_out)

                # Simulierte Messwerte (Demo):
                times = pd.date_range(current_date, periods=10, freq="H")
                energies = np.random.rand(10, 5)  # 5 Kanäle
                df = pd.DataFrame(energies, index=times, columns=[f"Channel_{i}" for i in range(1, 6)])
                all_data.append(df)

        except Exception as e:
            print(f"Fehler beim Download {date_str}: {e}")

        current_date += dt.timedelta(days=1)

    if not all_data:
        return None
    
    result_df = pd.concat(all_data).sort_index()
    return result_df.reset_index().rename(columns={"index": "time"})

@app.route("/data")
def get_data():
    start = request.args.get("start")
    end = request.args.get("end")
    try:
        start_dt = dt.datetime.strptime(start, "%Y-%m-%d")
        end_dt = dt.datetime.strptime(end, "%Y-%m-%d")
    except Exception:
        return jsonify({"error": "Ungültiges Datum. Bitte Format YYYY-MM-DD verwenden."}), 400

    df = download_and_extract_goes18_data(start_dt, end_dt)
    if df is None:
        return jsonify({"error": "Keine Daten gefunden."}), 404

    return df.to_json(orient="records", date_format="iso")

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        conn = get_db_connection()
        user = conn.execute(
            'SELECT * FROM users WHERE username = ?', (username,)
        ).fetchone()
        conn.close()

        if user and check_password_hash(user['password'], password):
            session['user_id'] = user['id']  # Nutzer in der Session speichern
            flash('Erfolgreich angemeldet!')
            return redirect(url_for('dashboard'))  # ← Hier leitest du weiter!
        else:
            flash('Ungültiger Benutzername oder Passwort.')
            return redirect(url_for('login'))

    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop("username", None)
    return redirect(url_for('login'))

@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('dashboard.html')


if __name__ == '__main__':
    init_db()
    get_db_connection()
    app.run(host="0.0.0.0", port=5000)
