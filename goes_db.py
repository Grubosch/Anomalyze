import os
import requests
from bs4 import BeautifulSoup
import xarray as xr
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from urllib.parse import urljoin
from sqlalchemy import text

# KONFIGURATION
GOES_SATELLITES = ["goes16", "goes17", "goes18", "goes19"]
BASE_URL = "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/"
DB_URI = "postgresql+psycopg2://goes:zbS2M@localhost/goes_data"  
LOCAL_DOWNLOAD_DIR = "./downloads"

def ensure_download_dir():
    if not os.path.exists(LOCAL_DOWNLOAD_DIR):
        os.makedirs(LOCAL_DOWNLOAD_DIR)

def list_nc_files_for_day(sat, year, month, day):
    url = f"{BASE_URL}{sat}/l1b/seis-l1b-sgps/{year}/{month:02}/{day:02}/"
    response = requests.get(url)
    if response.status_code != 200:
        print(f"[{sat}] Keine Daten für {year}-{month:02}-{day:02}")
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    files = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.nc')]
    return [urljoin(url, f) for f in files]

def download_file(url, local_path):
    r = requests.get(url, stream=True)
    with open(local_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

def parse_nc_and_store(local_file, sat, engine):
    ds = xr.open_dataset(local_file)
    if 'time' not in ds:
        print(f"[WARN] Keine Zeitvariable in {local_file}")
        return

    # Zeit und Beispielkanäle auslesen (je nach Datei evtl. andere Variablennamen!)
    time = ds['time'].values
    columns = {'satellite': [], 'time': []}
    
    # nur Flüsse extrahieren:
    flux_vars = [var for var in ds.variables if 'flux' in var.lower()]
    for var in flux_vars:
        columns[var] = []

    for t_idx, t in enumerate(time):
        columns['satellite'].append(sat)
        columns['time'].append(pd.Timestamp(t).to_pydatetime())
        for var in flux_vars:
            try:
                value = ds[var].values[t_idx]
                columns[var].append(float(value))
            except Exception:
                columns[var].append(None)

    df = pd.DataFrame(columns)
    df.to_sql("particle_flux", engine, if_exists='append', index=False)
    print(f"[OK] Daten aus {local_file} in DB gespeichert.")


def already_downloaded(conn, satellite, date):
    query = text("SELECT 1 FROM particle_flux WHERE satellite = :satellite AND DATE(time) = :date LIMIT 1;")
    res = conn.execute(query, {"satellite": satellite, "date": date}).fetchone()
    return res is not None

def main():
    today = datetime.utcnow().date()
    year, month, day = today.year, today.month, today.day
    ensure_download_dir()

    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name = 'particle_flux';"))
        for row in result:
            print(row)
    conn=engine.connect()

    for sat in GOES_SATELLITES:
        if already_downloaded(conn, sat, today):
            print(f"[{sat}] Daten für heute {today} schon in DB.")
            continue

        files = list_nc_files_for_day(sat, year, month, day)
        if not files:
            continue

        for file_url in files:
            local_filename = os.path.join(LOCAL_DOWNLOAD_DIR, f"{sat}_{os.path.basename(file_url)}")
            print(f"[{sat}] Lade {file_url}...")
            download_file(file_url, local_filename)
            parse_nc_and_store(local_filename, sat, engine)
            os.remove(local_filename)  # Platz sparen

if __name__ == "__main__":
    main()
