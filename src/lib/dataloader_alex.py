import pandas as pd
import geopandas as gpd

PATH_PARCELS = r"../data/extracted_parcels/Parcels_NEW.shp"
PATH_DU_EST = r"../data/parcels_cleanup.csv"


def get_parcels():
    result_df = pd.read_file("../data/extracted_parcels/Parcels_NEW.shp")
    return result_df


def get_du_est():
    result_df = pd.read_csv("../data/parcels_cleanup.csv")
    return result_df
