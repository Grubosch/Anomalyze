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

def process_and_store_nc(conn, sat, filepath):

def process_and_store_nc(conn, sat, filepath):
    ds = netCDF4.Dataset(filepath)
    
    # Zeit auslesen
    times = ds.variables['L1a_SciData_TimeStamp'][:]
    time_units = ds.variables['L1a_SciData_TimeStamp'].units
    base_time = netCDF4.num2date(times, time_units)
    
    # Energiewerte aus einem passenden Array (hier als Beispiel L1a_EngData_Flag rausgenommen, lieber energy_T1 nehmen)
    energy = ds.variables['energy_T1'][:]  # Energie-Bins für T1, ähnlich für T2 und T3
    
    # Protonfluss-Daten auslesen
    flux_T1 = ds.variables['T1_DifferentialProtonFluxes'][:]  # dims: Zeit x Energie
    flux_T2 = ds.variables['T2_DifferentialProtonFluxes'][:]
    flux_T3 = ds.variables['T3_DifferentialProtonFluxes'][:]

    for t_idx, time_val in enumerate(base_time):
        for e_idx, energy_val in enumerate(energy):
            # T1
            flux_val = flux_T1[t_idx, e_idx]
            if not np.isnan(flux_val):
                ins = text("""
                    INSERT INTO particle_flux (satellite, time, species, energy, flux)
                    VALUES (:satellite, :time, :species, :energy, :flux)
                """)
                conn.execute(ins, {
                    "satellite": sat,
                    "time": time_val,
                    "species": "T1",
                    "energy": float(energy_val),
                    "flux": float(flux_val)
                })
            # T2
            flux_val = flux_T2[t_idx, e_idx]
            if not np.isnan(flux_val):
                conn.execute(ins, {
                    "satellite": sat,
                    "time": time_val,
                    "species": "T2",
                    "energy": float(energy_val),
                    "flux": float(flux_val)
                })
            # T3
            flux_val = flux_T3[t_idx, e_idx]
            if not np.isnan(flux_val):
                conn.execute(ins, {
                    "satellite": sat,
                    "time": time_val,
                    "species": "T3",
                    "energy": float(energy_val),
                    "flux": float(flux_val)
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
