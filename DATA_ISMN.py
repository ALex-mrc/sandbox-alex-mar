from ismn.interface import ISMN_Interface
import matplotlib.pyplot as plt
import pandas as pd
import tqdm
import os
import re

import folium
from folium.plugins import MarkerCluster
import base64
import io


data_path = "/home/alex/Documents/Projet_Stage/PROJET/DATA/Data_separate_files_header_20210101_20211231_13268_65EF_20260330.zip"
data_path2 = "/home/alex/Documents/Projet_Stage/PROJET/DATA/Data_separate_files_header_20210101_20211231_13268_AWVn_20260330.zip"
out_dir = "/home/alex/Documents/Projet_Stage/PROJET/DATA/Stations_insitu"
PERIODE = ("2021-05-01", "2021-05-31")

os.makedirs(out_dir, exist_ok=True)





def coord_station_ismn(data):
    ismn_data = ISMN_Interface(data)
    coordonnees_stations = []
    stations = []
    for network in ismn_data.networks.values():
        for station in network.iter_stations():
            lat = station.lat() if callable(station.lat) else station.lat
            lon = station.lon() if callable(station.lon) else station.lon
            coordonnees_stations.append((station.name, lat, lon))
            stations.append(station.name)
            print(f"Station {station.name}: lat={lat}, lon={lon}")
    return coordonnees_stations, stations

#print(coord_station_ismn(data_path2))


def extract_bounding_box(df):
    lat_min = df["Lat"].min()
    lat_max = df["Lat"].max()
    lon_min = df["Lon"].min()
    lon_max = df["Lon"].max()
    return lat_min, lat_max, lon_min, lon_max

#print(extract_bounding_box(pd.read_excel("/home/alex/Documents/Projet_Stage/PROJET/DATA/Stations_insitu/stations_mai2021.xlsx")))




def parse_sensor_name(name):

    m = re.match(r'^([^_]+)_.+_(\d+\.\d+)_(\d+\.\d+)$', name)

    if m:
        instrument = m.group(1)
        profondeur_cm = round((float(m.group(2)) + float(m.group(3))) / 2 * 100, 1)

    else:
        instrument = name
        profondeur_cm = None

    return instrument, profondeur_cm

def extract_station_data(data_path, periode, stations=None , sensor=None):

    ismn_data = ISMN_Interface(data_path)
    rows = []
    for network in ismn_data.networks.values():

        for station in tqdm.tqdm(network.iter_stations(), total=len(network.stations), desc=f"{network.name}", unit="Station"):

            if stations is not None and station.name not in stations:

                continue

            lat = station.lat() if callable(station.lat) else station.lat
            lon = station.lon() if callable(station.lon) else station.lon

            for sensor_obj in station.iter_sensors():
                instrument, profondeur_cm = parse_sensor_name(sensor_obj.name)
                if sensor is not None and instrument != sensor:
                    continue

                df = sensor_obj.read_data()
                df.index = pd.to_datetime(df.index)
                df = df.loc[periode[0]:periode[1]]

                if not df.empty:
                    col = df.columns[0]

                    for date, row in df.iterrows():

                        rows.append({"station": station.name,"Capteur": instrument,
                                    "Profondeur_cm": profondeur_cm,"Lat": lat,
                                    "Lon": lon,"Date": date,"Soil_moisture": row[col]})
                        
    return pd.DataFrame(rows)

sensor = "CS655-A"
stations = ["SOD140"]
tab = extract_station_data(data_path2, PERIODE, stations=stations, sensor=sensor)    
tab.to_excel("/home/alex/Documents/Projet_Stage/PROJET/DATA/Stations_insitu/stations_SOD140_mai2021.xlsx", index=False)



def plot_station_data(df_path, stations=None, sensor=None):

    df = pd.read_excel(df_path)
    if stations is not None:
        df = df[df["station"].isin(stations)]
    if sensor is not None:
        df = df[df["Capteur"].isin(sensor if isinstance(sensor, list) else [sensor])]

    for station_name, group in df.groupby("station"):
        fig, ax = plt.subplots(figsize=(12, 4))

        for sensor_name, sensor_group in group.groupby("Profondeur_cm"):
            ax.plot(sensor_group["Date"], sensor_group["Soil_moisture"], label=f"{sensor_name} cm", linewidth=0.8)

        ax.set_title(f"{station_name}")
        ax.set_xlabel("Date")
        ax.set_ylabel("Humidité du sol (m3/m3)")
        ax.tick_params(axis='x', rotation=30)

        plt.grid(True, linestyle='--', alpha=0.5)
        plt.legend(title="Profondeur (cm)", fontsize=8)
        plt.tight_layout()
        plt.savefig(f"{out_dir}/{station_name}_mai2021.png")
        plt.close()

sensor = "CS655-A"
plot_station_data("/home/alex/Documents/Projet_Stage/PROJET/DATA/Stations_insitu/stations_SOD140_mai2021.xlsx", stations=None, sensor=sensor)



def plot_station_by_profondeur(df_path, stations=None, profondeur=None):

    df = pd.read_excel(df_path)
    if stations is not None:
        df = df[df["station"].isin(stations)]
    if profondeur is not None:
        df = df[df["Profondeur_cm"].isin(profondeur if isinstance(profondeur, list) else [profondeur])]

    for depth, group in df.groupby("Profondeur_cm"):
        fig, ax = plt.subplots(figsize=(12, 4))

        for station_name, station_group in group.groupby("station"):
            ax.plot(station_group["Date"], station_group["Soil_moisture"], label=station_name, linewidth=0.8)

        ax.set_title(f"Profondeur : {depth} cm")
        ax.set_xlabel("Date")
        ax.set_ylabel("Humidité du sol (m³/m³)")
        ax.tick_params(axis='x', rotation=30)


        plt.grid(True, linestyle='--', alpha=0.5)
        plt.legend(title="Station", fontsize=8, loc='upper right', ncol=2)
        plt.tight_layout()
        plt.savefig(f"{out_dir}/profondeur_{depth}cm_mai2021.png")
        plt.close()


#plot_station_by_profondeur("/home/alex/Documents/Projet_Stage/PROJET/DATA/Stations_insitu/stations_SOD140_mai2021.xlsx", stations=None, profondeur=[10.0])



def plot_station(df_path, stations=None):

    df = pd.read_excel(df_path)

    if stations is not None:
        df = df[df["station"].isin(stations)]

    m = folium.Map(location=[df["Lat"].mean(), df["Lon"].mean()], zoom_start=6)
    marker_cluster = MarkerCluster().add_to(m)

    for station_name, group in df.groupby("station"):
        lat = group["Lat"].iloc[0]
        lon = group["Lon"].iloc[0]

        stats = (group.groupby("Profondeur_cm")["Soil_moisture"].agg(Moyenne="mean", Ecart_type="std", Min="min", Max="max")
            .round(4).reset_index())
        
        stats_html = stats.to_html(index=False, border=1,classes="stats", justify="center")


        fig, ax = plt.subplots(figsize=(6, 3))

        for depth, dgroup in group.groupby("Profondeur_cm"):
            ax.plot(pd.to_datetime(dgroup["Date"]), dgroup["Soil_moisture"],linewidth=0.8, label=f"{depth} cm")
            
        ax.set_title(station_name, fontsize=9)
        ax.set_xlabel("Date", fontsize=7)
        ax.set_ylabel("Humidité (m³/m³)", fontsize=7)
        ax.tick_params(axis='x', rotation=30, labelsize=6)
        ax.tick_params(axis='y', labelsize=6)
        ax.legend(title="Prof.", fontsize=6, title_fontsize=6)
        ax.grid(True, linestyle='--', alpha=0.4)

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=100)
        plt.close(fig)
        buf.seek(0)

        img_b64 = base64.b64encode(buf.read()).decode("utf-8")
        img_html = f'<img src="data:image/png;base64,{img_b64}" width="420"/>'


        popup_html = f"""
        <div style="font-family:Arial; font-size:12px; width:440px">
          <h4 style="margin:4px 0">{station_name}</h4>
          <b>Lat :</b> {lat:.4f} &nbsp; <b>Lon :</b> {lon:.4f}<br><br>
          <b>Statistiques descriptives</b><br>
          <style>.stats {{border-collapse:collapse; width:100%; font-size:11px}}
                 .stats td, .stats th {{border:1px solid #ccc; padding:3px 6px}}
                 .stats th {{background:#f0f0f0}}</style>
          {stats_html}
          <br>{img_html}
        </div>"""
        folium.Marker(location=[lat, lon],popup=folium.Popup(popup_html, max_width=460),tooltip=station_name,).add_to(marker_cluster)

    m.save(f"{out_dir}/stations_SOD140_map_mai2021.html")
    

#plot_station("/home/alex/Documents/Projet_Stage/PROJET/DATA/Stations_insitu/stations_SOD140_mai2021.xlsx", stations=None)
