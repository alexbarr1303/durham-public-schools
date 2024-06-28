import pandas as pd
import geopandas as gpd

PATH_PARCELS = r"../data/extracted_parcels"
PATH_DU_EST = r"../data/parcels_cleanup.csv"


def get_parcels():
    parcels_df = gpd.read_file(PATH_PARCELS)
    return parcels_df


def get_du_est():
    du_df = pd.read_csv(PATH_DU_EST)
    return du_df

def add_columns_from_csv(gdf, csv):

    gdf = gdf.merge(csv[['REID', 'designation', 'housing_type', 'du_est_parcels']],   
                    left_on='REID', right_on='REID', how='left')
    
    return gdf
