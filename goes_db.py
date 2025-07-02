import os
import requests
import datetime
import netCDF4
import numpy as np
from sqlalchemy import create_engine, text
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import re


# Datenbankverbindung (dein Passwort hier einsetzen)
DB_URI = "postgresql+psycopg2://goes:zbS2M@localhost/goes_data"
engine = create_engine(DB_URI)
# Satelliten-URLs
SATELLITES = [
    "goes16",
    "goes17",
    "goes18",
    "goes19",
]

BASE_URL = "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/{sat}/l1b/seis-l1b-sgps/"

def already_downloaded(conn, satellite, date):
    query = text("""
        SELECT 1 FROM particle_flux
        WHERE satellite = :satellite AND DATE(time) = :date LIMIT 1
    """)
    res = conn.execute(query, {"satellite": satellite, "date": date}).fetchone()
    return res is not None

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        if r.status_code != 200:
            return False
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return True



def convert_time(t):
    # Falls MaskedArray, hole rohen Wert
    if isinstance(t, np.ma.MaskedArray):
        t = t.data
    # Falls cftime-Objekt, in datetime konvertieren
    if hasattr(t, 'isoformat'):
        return datetime.fromisoformat(t.isoformat())
    return t

def process_and_store_nc(conn, sat, filepath):
    ds = netCDF4.Dataset(filepath)
    
    # Zeitwerte auslesen
    times = ds.variables['L1a_SciData_TimeStamp'][:]
    time_units = ds.variables['L1a_SciData_TimeStamp'].units
    base_time = netCDF4.num2date(times, time_units)
    
    # Protonenflussdaten auslesen
    flux_T1 = ds.variables['T1_DifferentialProtonFluxes'][:]
    flux_T2 = ds.variables['T2_DifferentialProtonFluxes'][:]
    flux_T3 = ds.variables['T3_DifferentialProtonFluxes'][:]

    ins = text("""
        INSERT INTO particle_flux (satellite, time, species, flux)
        VALUES (:satellite, :time, :species, :flux)
    """)
    
    for t_idx in range(len(base_time)):
        time_val = base_time[t_idx][0]
        print(time_val)
        time_val = convert_time(time_val)

        flux_sum_T1 = np.nansum(flux_T1[t_idx, :])
        flux_sum_T2 = np.nansum(flux_T2[t_idx, :])
        flux_sum_T3 = np.nansum(flux_T3[t_idx, :])
        
        if not np.isnan(flux_sum_T1):
            conn.execute(ins, {
                "satellite": sat,
                "time": time_val,
                "species": "T1",
                "flux": float(flux_sum_T1)
            })
        if not np.isnan(flux_sum_T2):
            conn.execute(ins, {
                "satellite": sat,
                "time": time_val,
                "species": "T2",
                "flux": float(flux_sum_T2)
            })
        if not np.isnan(flux_sum_T3):
            conn.execute(ins, {
                "satellite": sat,
                "time": time_val,
                "species": "T3",
                "flux": float(flux_sum_T3)
            })

def main():
    engine = create_engine(DB_URI)
    for sat in SATELLITES:
        print(f"Starte Download für {sat}...")
        url_base = BASE_URL.format(sat=sat)

        resp = requests.get(url_base)
        if resp.status_code != 200:
            print(f"Konnte Index-Seite nicht öffnen: {url_base}")
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        years = [link.get('href').rstrip('/') for link in soup.find_all('a')
                if link.get('href') and link.get('href').startswith('20') and link.get('href').endswith('/')]

        for year in years:
            year_url = urljoin(url_base, f"{year}/")
            resp_year = requests.get(year_url)
            if resp_year.status_code != 200:
                continue

            soup_year = BeautifulSoup(resp_year.text, 'html.parser')
            months = [link.get('href').rstrip('/') for link in soup_year.find_all('a')
                    if link.get('href') and re.match(r'\d{2}/', link.get('href'))]

            for month in months:
                month_url = urljoin(year_url, f"{month}/")
                resp_month = requests.get(month_url)
                if resp_month.status_code != 200:
                    continue

                soup_month = BeautifulSoup(resp_month.text, 'html.parser')
                nc_files = [link.get('href') for link in soup_month.find_all('a')
                            if link.get('href') and link.get('href').endswith('.nc')]

                for nc_file in nc_files:
                    match = re.search(r'(\d{8})', nc_file)
                    if not match:
                        print(f"Konnte kein Datum im Dateinamen finden: {nc_file}")
                        continue
                    date_str = match.group(1)
                    file_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()

                    with engine.begin() as conn:
                        if already_downloaded(conn, sat, file_date):
                            continue

                    file_url = urljoin(month_url, nc_file)
                    local_filename = f"/tmp/{sat}_{nc_file}"

                    print(f"Download: {file_url}")
                    if download_file(file_url, local_filename):
                        print(f"Verarbeite: {local_filename}")
                        try:
                            with engine.begin() as conn:
                                process_and_store_nc(conn, sat, local_filename)
                        except Exception as e:
                            print(f"Fehler beim Verarbeiten: {e}")
                        finally:
                            os.remove(local_filename)
                    else:
                        print(f"Download fehlgeschlagen: {file_url}")

if __name__ == "__main__":
    main()
