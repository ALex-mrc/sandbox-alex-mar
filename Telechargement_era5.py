import cdsapi
import time 

c = cdsapi.Client()


start = time.time()
c.retrieve(
    "reanalysis-era5-land",
    {
        "variable": [
            "volumetric_soil_water_layer_1",  # 0–7 cm
            "volumetric_soil_water_layer_2",  # 7–28 cm
            "volumetric_soil_water_layer_3",  # 28–100 cm
        ],
        "product_type": "reanalysis",
        "year": "2021",
        "month": [f"{m:02d}" for m in range(1, 13)],
        "day": [f"{d:02d}" for d in range(1, 32)],
        "time": ["00:00", "06:00", "12:00", "18:00"],
        "area": [67.38431, 26.62515, 67.15288, 26.74857], 
        "data_format": "netcdf",
    },
    "era5_land_soil_moisture_2021.nc"
)
end = time.time()

print((end - start)*60)