import pandas as pd
import geopandas as gpd

PATH_PARCELS = r"../../data/Parcels_1"
PATH_DU_EST = r"../../data/parcels_cleanup.csv"


def get_parcels():
    parcels_df = gpd.read_file(PATH_PARCELS)
    return parcels_df


def get_du_est():
    du_df = pd.read_csv(PATH_DU_EST)
    return du_df


def add_columns_from_csv(gdf, csv):

    gdf["REID"] = gdf["REID"].astype(str)
    csv["REID"] = csv["REID"].astype(str)

    merged_csv_gdf = gdf.merge(
        csv[
            [
                "REID",
                "designation",
                "housing_type",
                "du_est_final",
                "students2324",
                "students2223",
                "students2122",
                "students2021",
                "geo_id_b2010",
                "geo_id_b2020",
                "geo_id_bg2010",
                "geo_id_bg2020",
                "sch_id_base1819_es",
                "sch_id_base_es",
                "sch_id_gt_es",
                "sch_id_yr_es",
                "sch_id_yr_optout_es",
                "sch_id_zone",
                "sch_id_base_hs",
                "sch_id_gt_hs",
                "sch_id_base1819_ms",
                "sch_id_base_ms",
                "sch_id_gt_ms",
                "sch_id_yr_ms",
                "pu_2122_833",
                "pu_2324_848",
                "geo_id_t2010" "geo_id_t2020",
            ]
        ],
        left_on="REID",
        right_on="REID",
        how="right",
    )

    return merged_csv_gdf
