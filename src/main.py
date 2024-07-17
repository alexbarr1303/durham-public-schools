# functions to run R code for fetching census data
import rpy2.robjects as robjects
import pandas as pd
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

    # CENSUS ==================================================================
    robjects.r("source('src/lib/DataGathering.r')")

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

    # Joins ===================================================================

    base_dataset = add_columns_from_csv(durham_open, parcels_clean)

    # converting geo_ids to integers for joining with census data
    base_dataset["geo_id_t2020"] = fix_geoid_dtypes(base_dataset["geo_id_t2020"])
    base_dataset["geo_id_b2020"] = fix_geoid_dtypes(base_dataset["geo_id_b2020"])
    base_dataset["geo_id_bg2020"] = fix_geoid_dtypes(base_dataset["geo_id_bg2020"])

    # adding columns from census data
    base_dataset = add_columns_from_census(base_dataset, acs_table_t, "t")
    base_dataset = add_columns_from_census(base_dataset, acs_table_bg, "bg")

    # Calculations ============================================================
    base_dataset = clean_analytic_dataset(base_dataset)

    base_dataset = process_data(base_dataset)

    base_dataset.to_csv("../data/test.csv")
