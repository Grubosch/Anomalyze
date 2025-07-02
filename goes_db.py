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
    # Falls MaskedArray, rohen Wert extrahieren
    if isinstance(t, np.ma.MaskedArray):
        t = t.data
    # Falls cftime-Objekt, in datetime umwandeln
    if hasattr(t, 'year') and hasattr(t, 'month') and hasattr(t, 'day'):
        return datetime.datetime(t.year, t.month, t.day, t.hour, t.minute, t.second)
    return t

def process_and_store_nc(conn, sat, filepath):
    ds = netCDF4.Dataset(filepath)
    
    # Zeitwerte auslesen
    times = ds.variables['L1a_SciData_TimeStamp'][:]
    time_units = ds.variables['L1a_SciData_TimeStamp'].units
    base_time = netCDF4.num2date(times, time_units)
    
    # Protonenflussdaten auslesen
    pro_flux_T1 = ds.variables['T1_DifferentialProtonFluxes'][:]
    pro_flux_T2 = ds.variables['T2_DifferentialProtonFluxes'][:]
    pro_flux_T3 = ds.variables['T3_DifferentialProtonFluxes'][:]

    pro_flux_T1_unc = ds.variables['T1_DifferentialProtonFluxUncertainties'][:]
    pro_flux_T2_unc = ds.variables['T2_DifferentialProtonFluxUncertainties'][:]
    pro_flux_T3_unc = ds.variables['T3_DifferentialProtonFluxUncertainties'][:]
    # Alphaflussdaten auslesen
    alp_flux_T1 = ds.variables['T1_DifferentialAlphaFluxes'][:]
    alp_flux_T2 = ds.variables['T2_DifferentialAlphaFluxes'][:]
    alp_flux_T3 = ds.variables['T3_DifferentialAlphaFluxes'][:]

    alp_flux_T1_unc = ds.variables['T1_DifferentialAlphaFluxUncertainties'][:]
    alp_flux_T2_unc = ds.variables['T2_DifferentialAlphaFluxUncertainties'][:]
    alp_flux_T3_unc = ds.variables['T3_DifferentialAlphaFluxUncertainties'][:]

    ins = text("""
        INSERT INTO particle_flux (satellite, time, species, ProtonT1.1, ProtonT1.1_unc, AlphaT1.1, AlphaT1.1_unc, ProtonT1.2, ProtonT1.2_unc, AlphaT1.2, AlphaT1.2_unc, ProtonT1.3, ProtonT1.3_unc, AlphaT1.3, AlphaT1.3_unc, ProtonT1.4, ProtonT1.4_unc, AlphaT1.4, AlphaT1.4_unc, ProtonT1.5, ProtonT1.5_unc, AlphaT1.5, AlphaT1.5_unc, ProtonT1.6, ProtonT1.6_unc, AlphaT1.6, AlphaT1.6_unc, ProtonT2.1, ProtonT2.1_unc, AlphaT2.1, AlphaT2.1_unc, ProtonT2.2, ProtonT2.2_unc, AlphaT2.2, AlphaT2.2_unc, ProtonT3.1, ProtonT3.1_unc, AlphaT3.1, AlphaT3.1_unc, ProtonT3.2, ProtonT3.2_unc, AlphaT3.2, AlphaT3.2_unc, ProtonT3.3, ProtonT3.3_unc, AlphaT3.3, AlphaT3.3_unc, ProtonT3.4, ProtonT3.4_unc, AlphaT3.4, AlphaT3.4_unc, ProtonT3.5, ProtonT3.5_unc, AlphaT3.5, AlphaT3.5_unc)
        VALUES (:satellite, :time, :species, :ProtonT1.1, :ProtonT1.1_unc, :AlphaT1.1, :AlphaT1.1_unc, :ProtonT1.2, :ProtonT1.2_unc, :AlphaT1.2, :AlphaT1.2_unc, :ProtonT1.3, :ProtonT1.3_unc, :AlphaT1.3, :AlphaT1.3_unc, :ProtonT1.4, :ProtonT1.4_unc, :AlphaT1.4, :AlphaT1.4_unc, :ProtonT1.5, :ProtonT1.5_unc, :AlphaT1.5, :AlphaT1.5_unc, :ProtonT1.6, :ProtonT1.6_unc, :AlphaT1.6, :AlphaT1.6_unc, :ProtonT2.1, :ProtonT2.1_unc, :AlphaT2.1, :AlphaT2.1_unc, :ProtonT2.2, :ProtonT2.2_unc, :AlphaT2.2, :AlphaT2.2_unc, :ProtonT3.1, :ProtonT3.1_unc, :AlphaT3.1, :AlphaT3.1_unc, :ProtonT3.2, :ProtonT3.2_unc, :AlphaT3.2, :AlphaT3.2_unc, :ProtonT3.3, :ProtonT3.3_unc, :AlphaT3.3, :AlphaT3.3_unc, :ProtonT3.4, :ProtonT3.4_unc, :AlphaT3.4, :AlphaT3.4_unc, :ProtonT3.5, :ProtonT3.5_unc, :AlphaT3.5, :AlphaT3.5_unc)
    """)
    
    for t_idx in range(len(base_time)):
        time_val = base_time[t_idx][0]
        time_val = convert_time(time_val)
        print(flux_T1[t_idx,:])
        print()
#proton flux for all Ts and for D1 and D2 plus uncertainties
        d1_T1_pro_flux=pro_flux_T1[t_idx,0]
        d1_T2_pro_flux=pro_flux_T2[t_idx,0]
        d1_T3_pro_flux=pro_flux_T3[t_idx,0]

        d1_T1_pro_flux_unc=pro_flux_T1_unc[t_idx,0]
        d1_T2_pro_flux_unc=pro_flux_T2_unc[t_idx,0]
        d1_T3_pro_flux_unc=pro_flux_T3_unc[t_idx,0]

        d2_T1_pro_flux=pro_flux_T1[t_idx,1]
        d2_T2_pro_flux=pro_flux_T2[t_idx,1]
        d2_T3_pro_flux=pro_flux_T3[t_idx,1]

        d2_T1_pro_flux_unc=pro_flux_T1_unc[t_idx,1]
        d2_T2_pro_flux_unc=pro_flux_T2_unc[t_idx,1]
        d2_T3_pro_flux_unc=pro_flux_T3_unc[t_idx,1]
#alpha flux for all Ts and for D1 and D2 plus uncertainties
        d1_T1_alp_flux=alp_flux_T1[t_idx,0]
        d1_T2_alp_flux=alp_flux_T2[t_idx,0]
        d1_T3_alp_flux=alp_flux_T3[t_idx,0]

        d1_T1_alp_flux_unc=alp_flux_T1_unc[t_idx,0]
        d1_T2_alp_flux_unc=alp_flux_T2_unc[t_idx,0]
        d1_T3_alp_flux_unc=alp_flux_T3_unc[t_idx,0]

        d2_T1_alp_flux=alp_flux_T1[t_idx,1]
        d2_T2_alp_flux=alp_flux_T2[t_idx,1]
        d2_T3_alp_flux=alp_flux_T3[t_idx,1]

        d2_T1_alp_flux_unc=alp_flux_T1_unc[t_idx,1]
        d2_T2_alp_flux_unc=alp_flux_T2_unc[t_idx,1]
        d2_T3_alp_flux_unc=alp_flux_T3_unc[t_idx,1]

        conn.execute(ins, {
            "satellite": sat,
            "time": time_val,
            "species": "D1",
            "ProtonT1.1":d1_T1_pro_flux[0],
            "ProtonT1.1_unc":d1_T1_pro_flux_unc[0],
            "AlphaT1.1":d1_T1_alp_flux[0], 
            "AlphaT1.1_unc":d1_T1_alp_flux_unc[0],
            "ProtonT1.2":d1_T1_pro_flux[1],
            "ProtonT1.2_unc":d1_T1_pro_flux_unc[1],
            "AlphaT1.2":d1_T1_alp_flux[1], 
            "AlphaT1.2_unc":d1_T1_alp_flux_unc[1],
            "ProtonT1.3":d1_T1_pro_flux[2],
            "ProtonT1.3_unc":d1_T1_pro_flux_unc[2],
            "AlphaT1.3":d1_T1_alp_flux[2], 
            "AlphaT1.3_unc":d1_T1_alp_flux_unc[2],
            "ProtonT1.4":d1_T1_pro_flux[3],
            "ProtonT1.4_unc":d1_T1_pro_flux_unc[3],
            "AlphaT1.4":d1_T1_alp_flux[3], 
            "AlphaT1.4_unc":d1_T1_alp_flux_unc[3],
            "ProtonT1.5":d1_T1_pro_flux[4],
            "ProtonT1.5_unc":d1_T1_pro_flux_unc[4],
            "AlphaT1.5":d1_T1_alp_flux[4], 
            "AlphaT1.5_unc":d1_T1_alp_flux_unc[4],
            "ProtonT1.6":d1_T1_pro_flux[5],
            "ProtonT1.6_unc":d1_T1_pro_flux_unc[5],
            "AlphaT1.6":d1_T1_alp_flux[5], 
            "AlphaT1.6_unc":d1_T1_alp_flux_unc[5],
            "ProtonT2.1":d1_T2_pro_flux[0],
            "ProtonT2.1_unc":d1_T2_pro_flux_unc[0],
            "AlphaT2.1":d1_T2_alp_flux[0], 
            "AlphaT2.1_unc":d1_T2_alp_flux_unc[0],
            "ProtonT2.2":d1_T2_pro_flux[1],
            "ProtonT2.2_unc":d1_T2_pro_flux_unc[1],
            "AlphaT2.2":d1_T2_alp_flux[1], 
            "AlphaT2.2_unc":d1_T2_alp_flux_unc[1],
            "ProtonT3.1":d1_T3_pro_flux[0],
            "ProtonT3.1_unc":d1_T3_pro_flux_unc[0],
            "AlphaT3.1":d1_T3_alp_flux[0], 
            "AlphaT3.1_unc":d1_T3_alp_flux_unc[0],
            "ProtonT3.2":d1_T3_pro_flux[1],
            "ProtonT3.2_unc":d1_T3_pro_flux_unc[1],
            "AlphaT3.2":d1_T3_alp_flux[1], 
            "AlphaT3.2_unc":d1_T3_alp_flux_unc[1],
            "ProtonT3.3":d1_T3_pro_flux[2],
            "ProtonT3.3_unc":d1_T3_pro_flux_unc[2],
            "AlphaT3.3":d1_T3_alp_flux[2], 
            "AlphaT3.3_unc":d1_T3_alp_flux_unc[2],
            "ProtonT3.4":d1_T3_pro_flux[3],
            "ProtonT3.4_unc":d1_T3_pro_flux_unc[3],
            "AlphaT3.4":d1_T3_alp_flux[3], 
            "AlphaT3.4_unc":d1_T3_alp_flux_unc[3],
            "ProtonT3.5":d1_T3_pro_flux[4],
            "ProtonT3.5_unc":d1_T3_pro_flux_unc[4],
            "AlphaT3.5":d1_T3_alp_flux[4], 
            "AlphaT3.5_unc":d1_T3_alp_flux_unc[4],
        })
        conn.execute(ins, {
            "satellite": sat,
            "time": time_val,
            "species": "D2",
            "ProtonT1.1":d2_T1_pro_flux[0],
            "ProtonT1.1_unc":d2_T1_pro_flux_unc[0],
            "AlphaT1.1":d2_T1_alp_flux[0], 
            "AlphaT1.1_unc":d2_T1_alp_flux_unc[0],
            "ProtonT1.2":d2_T1_pro_flux[1],
            "ProtonT1.2_unc":d2_T1_pro_flux_unc[1],
            "AlphaT1.2":d2_T1_alp_flux[1], 
            "AlphaT1.2_unc":d2_T1_alp_flux_unc[1],
            "ProtonT1.3":d2_T1_pro_flux[2],
            "ProtonT1.3_unc":d2_T1_pro_flux_unc[2],
            "AlphaT1.3":d2_T1_alp_flux[2], 
            "AlphaT1.3_unc":d2_T1_alp_flux_unc[2],
            "ProtonT1.4":d2_T1_pro_flux[3],
            "ProtonT1.4_unc":d2_T1_pro_flux_unc[3],
            "AlphaT1.4":d2_T1_alp_flux[3], 
            "AlphaT1.4_unc":d2_T1_alp_flux_unc[3],
            "ProtonT1.5":d2_T1_pro_flux[4],
            "ProtonT1.5_unc":d2_T1_pro_flux_unc[4],
            "AlphaT1.5":d2_T1_alp_flux[4], 
            "AlphaT1.5_unc":d2_T1_alp_flux_unc[4],
            "ProtonT1.6":d2_T1_pro_flux[5],
            "ProtonT1.6_unc":d2_T1_pro_flux_unc[5],
            "AlphaT1.6":d2_T1_alp_flux[5], 
            "AlphaT1.6_unc":d2_T1_alp_flux_unc[5],
            "ProtonT2.1":d2_T2_pro_flux[0],
            "ProtonT2.1_unc":d2_T2_pro_flux_unc[0],
            "AlphaT2.1":d2_T2_alp_flux[0], 
            "AlphaT2.1_unc":d2_T2_alp_flux_unc[0],
            "ProtonT2.2":d2_T2_pro_flux[1],
            "ProtonT2.2_unc":d2_T2_pro_flux_unc[1],
            "AlphaT2.2":d2_T2_alp_flux[1], 
            "AlphaT2.2_unc":d2_T2_alp_flux_unc[1],
            "ProtonT3.1":d2_T3_pro_flux[0],
            "ProtonT3.1_unc":d2_T3_pro_flux_unc[0],
            "AlphaT3.1":d2_T3_alp_flux[0], 
            "AlphaT3.1_unc":d2_T3_alp_flux_unc[0],
            "ProtonT3.2":d2_T3_pro_flux[1],
            "ProtonT3.2_unc":d2_T3_pro_flux_unc[1],
            "AlphaT3.2":d2_T3_alp_flux[1], 
            "AlphaT3.2_unc":d2_T3_alp_flux_unc[1],
            "ProtonT3.3":d2_T3_pro_flux[2],
            "ProtonT3.3_unc":d2_T3_pro_flux_unc[2],
            "AlphaT3.3":d2_T3_alp_flux[2], 
            "AlphaT3.3_unc":d2_T3_alp_flux_unc[2],
            "ProtonT3.4":d2_T3_pro_flux[3],
            "ProtonT3.4_unc":d2_T3_pro_flux_unc[3],
            "AlphaT3.4":d2_T3_alp_flux[3], 
            "AlphaT3.4_unc":d2_T3_alp_flux_unc[3],
            "ProtonT3.5":d2_T3_pro_flux[4],
            "ProtonT3.5_unc":d2_T3_pro_flux_unc[4],
            "AlphaT3.5":d2_T3_alp_flux[4], 
            "AlphaT3.5_unc":d2_T3_alp_flux_unc[4],
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
