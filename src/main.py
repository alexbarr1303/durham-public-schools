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
