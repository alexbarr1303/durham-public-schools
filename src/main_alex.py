import pandas as pd
import geopandas as gpd
from dataloader_alex import get_parcels, get_du_est, add_columns_from_csv

def main():
    # Load the parcels data
    parcels_df = get_parcels()
    
    # Load the du_est data
    du_df = get_du_est()
    
    # Merge the dataframes
    merged_gdf = add_columns_from_csv(parcels_df, du_df)
    
    return merged_gdf

if __name__ == "__main__":
    merged_gdf = main()
    # Print or save the merged GeoDataFrame as needed
    print(merged_gdf.head())


# call R script from Python
# get the census data by tract and bg in return
# source('/data_gathering.R')
# census_by_bg, census_by_t


# Join the census data by the midpoint spatal join rule
# join census with part 1 df


# aggregation
# - potential regression for the rent values
