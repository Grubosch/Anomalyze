import os
import requests
import datetime
import netCDF4
import numpy as np
from sqlalchemy import create_engine, text
from urllib.parse import urljoin

# Datenbankverbindung (dein Passwort hier einsetzen)
DB_URI = "postgresql+psycopg2://goes:zbS2M@localhost/goes_data"

# Satelliten-URLs
SATELLITES = [
    "goes16",
    "goes17",
    "goes18",
    "goes19",
]

BASE_URL = "https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/{sat}/l1b/seis-l1b-sgps/"

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
    ds = netCDF4.Dataset(filepath)
    times = ds.variables['time'][:]  # time dimension
    time_units = ds.variables['time'].units
    base_time = netCDF4.num2date(times, time_units)
    energy = ds.variables['energy'][:]  # energy levels
    flux = ds.variables['flux'][:]  # shape: time x species x energy
    species_var = ds.variables['species']  # variable with species names
    species_names = [s.tostring().decode('ascii').strip() for s in species_var[:]]

    # Insert each measurement
    for t_idx, time_val in enumerate(base_time):
        for s_idx, species in enumerate(species_names):
            for e_idx, energy_val in enumerate(energy):
                flux_val = flux[t_idx, s_idx, e_idx]
                if np.isnan(flux_val):
                    continue  # skip missing data
                ins = text("""
                    INSERT INTO particle_flux (satellite, time, species, energy, flux)
                    VALUES (:satellite, :time, :species, :energy, :flux)
                """)
                conn.execute(ins, {
                    "satellite": sat,
                    "time": time_val,
                    "species": species,
                    "energy": float(energy_val),
                    "flux": float(flux_val)
                })

def main():
    engine = create_engine(DB_URI)
    with engine.begin() as conn:  # transactionally safe
        for sat in SATELLITES:
            print(f"Starte Download für {sat}...")
            url_base = BASE_URL.format(sat=sat)

            # Lade die Index-Seite für die Jahre
            resp = requests.get(url_base)
            if resp.status_code != 200:
                print(f"Konnte Index-Seite nicht öffnen: {url_base}")
                continue

            # Finde alle Jahresordner (z.B. "2024/", "2025/") auf der Website
            years = [line.split('"')[1].rstrip('/')
                     for line in resp.text.splitlines()
                     if line.strip().startswith('<a href="20')]

            for year in years:
                year_url = urljoin(url_base, f"{year}/")
                resp_year = requests.get(year_url)
                if resp_year.status_code != 200:
                    continue
                # Finde alle .nc-Dateien
                nc_files = [line.split('"')[1]
                            for line in resp_year.text.splitlines()
                            if line.strip().endswith(".nc\">")]

                for nc_file in nc_files:
                    date_str = nc_file.split("_")[1]  # z.B. "20250701"
                    file_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()

                    if already_downloaded(conn, sat, file_date):
                        continue  # Daten schon vorhanden

                    file_url = urljoin(year_url, nc_file)
                    local_filename = f"/tmp/{sat}_{nc_file}"

                    print(f"Download: {file_url}")
                    if download_file(file_url, local_filename):
                        print(f"Verarbeite: {local_filename}")
                        try:
                            process_and_store_nc(conn, sat, local_filename)
                        except Exception as e:
                            print(f"Fehler beim Verarbeiten: {e}")
                        finally:
                            os.remove(local_filename)
                    else:
                        print(f"Download fehlgeschlagen: {file_url}")

if __name__ == "__main__":
    main()
