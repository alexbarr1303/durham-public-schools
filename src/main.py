import os
import geopandas as gpd
import pandas as pd
import fiona


# functions to run R code for fetching census data
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter

# functions for cleaning/manipulating data
from lib.data import (
    get_parcels,
    get_du_est,
    add_columns_from_csv,
    add_columns_from_census,
    fix_geoid_dtypes,
    clean_analytic_dataset,
    aggregate_by_geo_id,
)

# functions for aggregation, calculations/creating new variables
from lib.variables import process_data


# user-defined parameters
class CONFIG:
    CENSUS_YEAR = 2020
    PATH_PARCELS = r"data/Parcels_1"
    PATH_DU_EST = r"data/parcels_clean_duest_stu_spjoin_20240625.csv"

    PATH_DPS_LAYERS = r"data/dps_all_layers20240208.gdb"
    OUTPUT_DIR = r"data/outputs"
    OUTPUT_GDB_NAME = r"dps.gdb"  # must end in gdb
    layer_mapping = {
        # 'dps_all_layers_geo_id': 'base_dataset_geo_id'
        "b2020": "geo_id_b2020",
        "bg2020": "geo_id_bg2020",
        "t2020": "geo_id_t2020",
        "b2010": "geo_id_b2010",
        "bg2010": "geo_id_bg2010",
        "t2010": "geo_id_t2010",
        "PU_2324_848": "pu_2324_848",
    }

    # options for aggregation functions:
    # ['sum', 'mean', 'median', 'min', 'max', 'std']
    block_group_aggregations = {
        "du_est_final": ["sum", "mean"],
        "TOTAL_PROP_VALUE": ["sum", "mean"],
        "unit_val": ["sum", "mean"],
        # for the census columns, values are already aggregated on the corresponding geog ids,
        # and so, taking the mean will keep the values unchanged
        "estimate_rent_total_bg": "mean",
        "estimate_median_house_value_bg": "mean",
        "estimate_median_year_structure_build_bg": "mean",
        "estimate_housing_units_bg": "mean",
        "pct_vacant_bg": "mean",
        "pct_owner_occupied_bg": "mean",
    }

    tract_aggregations = {
        "du_est_final": ["sum", "mean"],
        "TOTAL_PROP_VALUE": ["sum", "mean"],
        "unit_val": ["sum", "mean"],
        # for the census columns, values are already aggregated on the corresponding geog ids,
        # and so, taking the mean will keep the values unchanged
        "estimate_rent_total_t": "mean",
        "estimate_median_house_value_t": "mean",
        "estimate_median_year_structure_build_t": "mean",
        "estimate_housing_units_t": "mean",
        "pct_vacant_t": "mean",
        "pct_owner_occupied_t": "mean",
    }

    aggregations = {
        "du_est_final": ["sum", "mean"],
        "TOTAL_PROP_VALUE": ["sum", "mean"],
        "unit_val": ["sum", "mean"],
    }


if __name__ == "__main__":

    if not os.path.exists(CONFIG.OUTPUT_DIR):
        os.makedirs(CONFIG.OUTPUT_DIR)
    else:
        pass

    gdb_path = os.path.join(CONFIG.OUTPUT_DIR, CONFIG.OUTPUT_GDB_NAME)

    # CENSUS ==================================================================
    robjects.r("source('src/lib/DataGathering.R')")

    make_acs_table_t_r = robjects.globalenv["make_acs_table_t"]
    make_acs_table_bg_r = robjects.globalenv["make_acs_table_bg"]

    # Convert the R DataFrame to a pandas DataFrame
    with localconverter(robjects.default_converter + pandas2ri.converter):
        make_acs_table_t = robjects.conversion.rpy2py(make_acs_table_t_r)
        make_acs_table_bg = robjects.conversion.rpy2py(make_acs_table_bg_r)

        acs_table_t_r = make_acs_table_t(CONFIG.CENSUS_YEAR)
        acs_table_bg_r = make_acs_table_bg(CONFIG.CENSUS_YEAR)

        acs_table_t = robjects.conversion.rpy2py(acs_table_t_r)
        acs_table_bg = robjects.conversion.rpy2py(acs_table_bg_r)

    # Durham Open/Parcels =====================================================

    durham_open = get_parcels(CONFIG.PATH_PARCELS)
    parcels_clean = get_du_est(CONFIG.PATH_DU_EST)

    # Build the base dataset ==================================================

    base_dataset = add_columns_from_csv(durham_open, parcels_clean)

    # converting geo_ids to integers for joining with census data
    base_dataset["geo_id_t2020"] = fix_geoid_dtypes(base_dataset["geo_id_t2020"])
    base_dataset["geo_id_b2020"] = fix_geoid_dtypes(base_dataset["geo_id_b2020"])
    base_dataset["geo_id_bg2020"] = fix_geoid_dtypes(base_dataset["geo_id_bg2020"])

    # adding columns from census data
    base_dataset = add_columns_from_census(base_dataset, acs_table_t, "t")
    base_dataset = add_columns_from_census(base_dataset, acs_table_bg, "bg")
    base_dataset = clean_analytic_dataset(base_dataset)
    base_dataset = process_data(base_dataset)

    # Join the base dataset with GDB layers ==================================

    for geo_layer in CONFIG.layer_mapping.keys():
        if geo_layer in ["bg2020", "bg2010"]:
            base_dataset_agg = aggregate_by_geo_id(
                base_dataset,
                CONFIG.layer_mapping[geo_layer],
                CONFIG.block_group_aggregations,
            )
            pass

        elif geo_layer in ["b2020", "b2010"]:
            base_dataset_agg = aggregate_by_geo_id(
                base_dataset, CONFIG.layer_mapping[geo_layer], CONFIG.aggregations
            )
            pass

        elif geo_layer in ["t2020", "t2010"]:
            base_dataset_agg = aggregate_by_geo_id(
                base_dataset, CONFIG.layer_mapping[geo_layer], CONFIG.tract_aggregations
            )
            pass

        else:
            base_dataset_agg = aggregate_by_geo_id(
                base_dataset, CONFIG.layer_mapping[geo_layer], CONFIG.aggregations
            )
            pass

        # read DPS all layers, and join the aggregated information
        # by corresponding geography
        gdf = gpd.read_file(CONFIG.PATH_DPS_LAYERS, layer=geo_layer)

        # ensure the datatypes of the geo id columns match before merging
        mapped_geo_col_name = CONFIG.layer_mapping[geo_layer]
        gdf.dropna(subset=mapped_geo_col_name, inplace=True)
        gdf[mapped_geo_col_name] = gdf[mapped_geo_col_name].astype(int)

        base_dataset_agg.dropna(subset=mapped_geo_col_name, inplace=True)
        base_dataset_agg[mapped_geo_col_name] = base_dataset_agg[
            mapped_geo_col_name
        ].astype(int)

        merged_gdf = gdf.merge(base_dataset_agg, how="left", on=mapped_geo_col_name)
        merged_gdf.crs = gdf.crs

        # merged_gdf.to_file(gdb_path, layer=geo_layer, driver="OpenFileGDB")

        # Convert the GeoDataFrame to a dict format compatible with fiona
        schema = {
            "geometry": "MultiPolygon",
            "properties": {
                col: (
                    "int"
                    if "int" in str(merged_gdf[col].dtype)
                    else "float" if "float" in str(merged_gdf[col].dtype) else "str"
                )
                for col in merged_gdf.columns
                if col != "geometry"
            },
        }

        # Write the merged GeoDataFrame to a new GDB as a new layer using fiona
        with fiona.open(
            gdb_path,
            "w",
            driver="OpenFileGDB",
            schema=schema,
            layer=geo_layer,
            crs=merged_gdf.crs,
        ) as layer:
            for idx, row in merged_gdf.iterrows():
                layer.write(
                    {
                        "geometry": row["geometry"].__geo_interface__,
                        "properties": {
                            col: row[col]
                            for col in merged_gdf.columns
                            if col != "geometry"
                        },
                    }
                )

        # Inspect the written layer
        print(f"Layer '{geo_layer}' written to {gdb_path}")
        pass
