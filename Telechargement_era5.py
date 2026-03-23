import cdsapi

c = cdsapi.Client()

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
        "month": ["05"],
        "day": [f"{d:02d}" for d in range(1, 32)],
        "time": ["00:00", "06:00", "12:00", "18:00"],
        "data_format": "netcdf",
    },
    "era5_land_soil_moisture_2021_05.nc"
)