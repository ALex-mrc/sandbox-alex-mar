import xarray as xr
import pandas as pd

def nc_to_csv(nc_file, csv_file):
    """
    Convertit un fichier NetCDF (.nc) en CSV
    
    Args:
        nc_file (str): Chemin du fichier .nc
        csv_file (str): Chemin du fichier CSV de sortie
    """
    # Lire le fichier NetCDF
    ds = xr.open_dataset(nc_file)
    
    # Convertir en DataFrame pandas
    df = ds.to_dataframe().reset_index()
    
    # Exporter en CSV
    df.to_csv(csv_file, index=False)
    print(f"Fichier converti avec succès : {csv_file}")

# Exemple d'utilisation
if __name__ == "__main__":
    nc_file = "votre_fichier.nc"
    csv_file = "sortie.csv"
    nc_to_csv(nc_file, csv_file)