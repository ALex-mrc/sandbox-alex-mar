from ismn.interface import ISMN_Interface
import matplotlib.pyplot as plt
import pandas as pd
import os
import re

data_path = "/home/alex/Téléchargements/Data_separate_files_header_20141006_20260131_13268_ZO05_20260322"
out_dir = "/home/alex/Documents/Projet_Stage/Plot/Stations_insitu"
PERIODE = ("2021-05-01", "2021-05-31")

os.makedirs(out_dir, exist_ok=True)

#afficher coordonnées des stations



ismn_data = ISMN_Interface(data_path)

sensors_data = []
for network in ismn_data.networks.values():
    for station in network.iter_stations():
        lat = station.lat() if callable(station.lat) else station.lat
        lon = station.lon() if callable(station.lon) else station.lon
        print(f"Station {station.name}: lat={lat}, lon={lon}")
        for sensor in station.iter_sensors():
            data = sensor.read_data()
            data.index = pd.to_datetime(data.index)
            data = data.loc[PERIODE[0]:PERIODE[1]]
            if not data.empty:
                sensors_data.append((station.name, sensor.name, data))


for station_name, sensor_name, data in sensors_data:
    col = data.columns[0]
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(data.index, data[col], linewidth=0.8)
    ax.set_title(f"{station_name} — {sensor_name}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Humidité du sol (m3/m3)")
    
    ax.tick_params(axis='x', rotation=30)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(f"{out_dir}/{station_name}_{sensor_name}_mai2021.png")
    plt.close()




n = len(sensors_data)
ncols = 2
nrows = -(-n // ncols)
fig, axes = plt.subplots(nrows, ncols, figsize=(14, 4 * nrows))
axes_flat = axes.flatten() if n > 1 else [axes]

for ax, (station_name, sensor_name, data) in zip(axes_flat, sensors_data):
    col = data.columns[0]
    ax.plot(data.index, data[col], linewidth=0.8)
    ax.set_title(f"{station_name} — {sensor_name}", fontsize=9)
    ax.set_xlabel("Date")
    ax.set_ylabel("Humidité du sol (m3/m3)")
    ax.tick_params(axis='x', rotation=30)

for ax in axes_flat[n:]:
    ax.set_visible(False)

plt.suptitle(f"Données ISMN - {PERIODE[0]} - {PERIODE[1]}", fontsize=14)
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.subplots_adjust(hspace=1.5)
plt.savefig(f"{out_dir}/Stations_mai2021_plot2.png")
plt.close()




def format_sensor_name(name):
    return re.sub(r'(\d+\.\d{2})\d+', r'\1', name)


fig, ax = plt.subplots(figsize=(14, 5))
for station_name, sensor_name, data in sensors_data:
    col = data.columns[0]
    ax.plot(data.index, data[col], linewidth=0.8, label=f"{station_name} - {format_sensor_name(sensor_name)}m")

ax.set_xlabel("Date")
ax.set_ylabel("Humidité du sol (m3/m3)")
ax.set_title(f"Données ISMN - {PERIODE[0]} - {PERIODE[1]}")
ax.legend(fontsize=7, loc="best")
ax.tick_params(axis='x', rotation=30)

plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()

plt.savefig(f"{out_dir}/stations_mai2021_plot.png")
plt.show()

