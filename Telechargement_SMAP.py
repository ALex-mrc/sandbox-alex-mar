import earthaccess
import h5py
import json
import numpy as np
import pandas as pd
from pathlib import Path

# === CONFIGURATION ===
LATITUDE  = 67.15288
LONGITUDE = 26.72914
START     = "2021-05-01"
END       = "2021-05-31"
OUTPUT    = Path("/home/alex/Documents/Projet_Stage/Data/SMAP")



def telecharger_smap_mai2021(lat, lon, output_dir):
    """
    Télécharge les données SMAP L3 Enhanced (9km) pour mai 2021
    autour d'un point géographique donné.
    """

    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Authentification Earthdata NASA ---
    earthaccess.login(strategy="interactive")  # saisie identifiants une seule fois

    # --- Recherche des granules SMAP L3 Enhanced (9 km, journalier) ---
    results = earthaccess.search_data(
        short_name   = "SPL3SMP_E",           # SMAP L3 Enhanced
        version      = "006",
        temporal     = (START, END),
        bounding_box = (lon - 0.5, lat - 0.5, lon + 0.5, lat + 0.5)  # bbox autour du point
    )

    print(f"{len(results)} fichiers trouvés pour mai 2021")

    # --- Téléchargement ---
    fichiers = earthaccess.download(results, local_path=output_dir)

    return fichiers


def _premier_dataset_disponible(group, noms_possibles):
    """Retourne le premier dataset existant parmi plusieurs noms possibles."""

    for nom in noms_possibles:
        if nom in group:
            return group[nom][:]
    return None


def extraire_soil_moisture(fichiers, lat, lon):
    """
    Extrait la valeur de soil moisture au point (lat, lon) pour chaque fichier HDF5.
    Retourne un DataFrame avec la série temporelle.
    """

    records = []

    for fichier in sorted(fichiers):
        with h5py.File(fichier, "r") as f:

            for passage in ["AM", "PM"]:
                group = f.get(f"Soil_Moisture_Retrieval_Data_{passage}")
                if group is None:
                    continue

                if passage == "AM":
                    sm_noms = ["soil_moisture", "soil_moisture_dca"]
                    lat_noms = ["latitude"]
                    lon_noms = ["longitude"]
                else:
                    sm_noms = ["soil_moisture_pm", "soil_moisture_dca_pm", "soil_moisture"]
                    lat_noms = ["latitude_pm", "latitude"]
                    lon_noms = ["longitude_pm", "longitude"]

                sm_data = _premier_dataset_disponible(group, sm_noms)
                lat_data = _premier_dataset_disponible(group, lat_noms)
                lon_data = _premier_dataset_disponible(group, lon_noms)

                if sm_data is None or lat_data is None or lon_data is None:
                    continue

                # Remplacer les valeurs manquantes
                sm_data[sm_data == -9999.0] = np.nan

                # Trouver le pixel le plus proche du point
                dist   = np.sqrt((lat_data - lat)**2 + (lon_data - lon)**2)
                idx    = np.unravel_index(np.nanargmin(dist), dist.shape)
                sm_val = sm_data[idx]
                dist_deg = float(dist[idx])
                dist_km = dist_deg * 111.32

                # Extraire la date depuis le nom du fichier
                # ex: SMAP_L3_SM_P_E_20210501_R18290_001.h5
                nom    = Path(fichier).stem
                date_str = nom.split("_")[5]
                date   = pd.to_datetime(date_str, format="%Y%m%d")

                records.append({
                    "date"          : date,
                    "passage"       : passage,
                    "soil_moisture" : sm_val,
                    "lat_pixel"     : lat_data[idx],
                    "lon_pixel"     : lon_data[idx],
                    "distance_deg"  : dist_deg,
                    "distance_km"   : dist_km,
                })

    df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)
    return df


def construire_pixel_quotidien_le_plus_proche(df):
    """
    Construit une table à 1 pixel par date (AM/PM confondus),
    en gardant le pixel avec la plus petite distance à la station.
    """

    df_valide = df.dropna(subset=["soil_moisture"]).copy()
    if df_valide.empty:
        return df_valide

    idx = df_valide.groupby("date")["distance_deg"].idxmin()
    return df_valide.loc[idx].sort_values("date").reset_index(drop=True)


def exporter_geojson_points(df, output_path, lat_station, lon_station):
    """Exporte un GeoJSON de points (EPSG:4326) compatible SIG."""

    features = []
    for row in df.itertuples(index=False):
        props = {
            "date": pd.to_datetime(row.date).strftime("%Y-%m-%d"),
            "passage": row.passage,
            "soil_moisture": None if pd.isna(row.soil_moisture) else float(row.soil_moisture),
            "distance_deg": float(row.distance_deg),
            "distance_km": float(row.distance_km),
            "lat_station": float(lat_station),
            "lon_station": float(lon_station),
        }
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row.lon_pixel), float(row.lat_pixel)],
                },
                "properties": props,
            }
        )

    geojson = {
        "type": "FeatureCollection",
        "name": output_path.stem,
        "crs": {
            "type": "name",
            "properties": {"name": "EPSG:4326"},
        },
        "features": features,
    }

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)


# === MAIN ===
if __name__ == "__main__":

    # 1. Téléchargement
    fichiers = telecharger_smap_mai2021(LATITUDE, LONGITUDE, OUTPUT)

    # 2. Extraction de la série temporelle
    df = extraire_soil_moisture(fichiers, LATITUDE, LONGITUDE)

    # 3. Affichage
    print(df.to_string(index=False))

    # 4. Sauvegarde CSV détaillé (AM + PM)
    csv_path = OUTPUT / "smap_soil_moisture_mai2021.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nSauvegardé CSV détaillé → {csv_path}")

    # 5. Table quotidienne: 1 pixel le plus proche par date
    df_daily = construire_pixel_quotidien_le_plus_proche(df)
    csv_daily_path = OUTPUT / "smap_soil_moisture_pixel_plus_proche_par_date.csv"
    df_daily.to_csv(csv_daily_path, index=False)
    print(f"Sauvegardé CSV quotidien → {csv_daily_path}")

    # 6. Export GeoJSON pour SIG
    geojson_all_path = OUTPUT / "smap_pixels_plus_proches_AM_PM.geojson"
    geojson_daily_path = OUTPUT / "smap_pixel_plus_proche_par_date.geojson"

    exporter_geojson_points(df, geojson_all_path, LATITUDE, LONGITUDE)
    exporter_geojson_points(df_daily, geojson_daily_path, LATITUDE, LONGITUDE)

    print(f"Sauvegardé GeoJSON AM/PM → {geojson_all_path}")
    print(f"Sauvegardé GeoJSON quotidien → {geojson_daily_path}")