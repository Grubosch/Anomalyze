from flask import Flask, render_template, request, redirect, url_for, flash, session

import sqlite3, hashlib
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = "dein_geheimes_schluessel"  # für Sessions, ändere das unbedingt

DATABASE = 'database.db'

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
    get_db_connection()
    app.run(host="0.0.0.0", port=5000)
