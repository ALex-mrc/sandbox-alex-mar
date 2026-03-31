
import zipfile
import numpy as np
import pandas as pd
import netCDF4 as nc

FILE_PATH = "/home/alex/Documents/Projet_Stage/Data/ERA5/era5_land_soil_moisture_2021_05.nc"

# Variables ERA5-Land : swvl1=0-7cm, swvl2=7-28cm, swvl3=28-100cm
VARIABLES = {
    "swvl1": "0-7 cm",
    "swvl2": "7-28 cm",
    "swvl3": "28-100 cm",
}

COORDS = [
    (67.1529, 26.7291)]


def _open_nc(file_path):
    """Ouvre le .nc même s'il est emballé dans un ZIP (format CDS)."""
    with zipfile.ZipFile(file_path) as z:
        data = z.read(z.namelist()[0])
    return nc.Dataset("in-mem", memory=data)


def extract_era5_at_coords(file_path, coords):
    """
    Extrait les valeurs ERA5 (swvl1/2/3) aux coordonnées les plus proches.

    Parameters
    ----------
    file_path : str
        Chemin vers le .nc (ou ZIP contenant un .nc).
    coords : list of (lat, lon)
        Liste de coordonnées WGS84.

    Returns
    -------
    pd.DataFrame avec colonnes : lat, lon, date, swvl1, swvl2, swvl3
    """
    ds = _open_nc(file_path)

    lats = ds.variables["latitude"][:]
    lons = ds.variables["longitude"][:]
    times = nc.num2date(ds.variables["valid_time"][:],
                        ds.variables["valid_time"].units)
    dates = pd.to_datetime([t.isoformat() for t in times])

    rows = []
    for lat, lon in coords:
        i_lat = int(np.argmin(np.abs(lats - lat)))
        i_lon = int(np.argmin(np.abs(lons - lon)))
        lat_nearest = float(lats[i_lat])
        lon_nearest = float(lons[i_lon])

        for t_idx, date in enumerate(dates):
            row = {"lat": lat_nearest, "lon": lon_nearest, "date": date}
            for var in VARIABLES:
                row[var] = float(ds.variables[var][t_idx, i_lat, i_lon])
            rows.append(row)

    ds.close()
    return pd.DataFrame(rows)


#df = extract_era5_at_coords(FILE_PATH, COORDS)
#df.to_excel("era5_soil_moisture_extracted.xlsx", index=False)

df = "/home/alex/Documents/Projet_Stage/era5_soil_moisture_extracted.xlsx"

read_df = pd.read_excel(df)
print(read_df.head())