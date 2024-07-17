import pandas as pd
import geopandas as gpd


def clean_analytic_dataset(df):

    subset_list = [
        # durham open
        "OBJECTID_1",
        "OBJECTID",
        "REID",
        "PIN",
        "PROPERTY_D",
        "LOCATION_A",
        "SPEC_DIST",
        "LAND_CLASS",
        "ACREAGE",
        "PROPERTY_O",
        "OWNER_MAIL",
        "geometry",
        # du est
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
        "geo_id_t2010",
        "geo_id_t2020",
        "region",
        "TOTAL_PROP_VALUE",
        # census t
        "estimate_rent_total_t",
        "moe_rent_total_t",
        "estimate_median_house_value_t",
        "estimate_median_year_structure_build_t",
        "estimate_housing_units_t",
        "pct_vacant_t",
        "pct_owner_occupied_t",
        # bg
        "estimate_rent_total_bg",
        "moe_rent_total_bg",
        "estimate_median_house_value_bg",
        "estimate_median_year_structure_build_bg",
        "estimate_housing_units_bg",
        "pct_vacant_bg",
        "pct_owner_occupied_bg",
    ]
    df_output = df[subset_list]
    df_output = df_output[df_output["du_est_final"] != 0]

    return df_output


def get_parcels(path):
    parcels_df = gpd.read_file(path)
    return parcels_df


def get_du_est(path):
    du_df = pd.read_csv(path)

    # Drop timestamp columns because they give trouble when writing out
    # as a geodatabase
    timestamp_cols = du_df.select_dtypes(include=["datetime64[ns]"]).columns
    du_df.drop(columns=timestamp_cols, inplace=True)
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
                "geo_id_t2010",
                "geo_id_t2020",
                "region",
                "TOTAL_PROP_VALUE",
            ]
        ],
        left_on="REID",
        right_on="REID",
        how="right",
    )

    merged_csv_gdf.dropna(subset="OBJECTID_1", inplace=True)

    return merged_csv_gdf


def fix_geoid_dtypes(series):
    return series.fillna(-1).astype(int).astype(str)


# Function to safely convert a column to int, fallback to str if it fails
def safe_convert_to_int(df, col_name):
    try:
        df[col_name] = df[col_name].astype(int)
    except ValueError:
        df[col_name] = df[col_name].astype(str)
    return df


def add_columns_from_census(gdf, csv, census_type="t"):

    census_col_list = [
        "GEOID",
        "estimate_rent_total",
        "moe_rent_total",
        "estimate_median_house_value",
        "estimate_median_year_structure_build",
        "estimate_housing_units",
        "pct_vacant",
        "pct_owner_occupied",
    ]

    csv = csv[census_col_list]

    if census_type == "t":

        csv = csv.add_suffix("_t")

        merged_csv_gdf = gdf.merge(
            csv,
            left_on="geo_id_t2020",
            right_on="GEOID_t",
            how="left",
        )

        merged_csv_gdf.drop(columns="GEOID_t", inplace=True)

        pass

    elif census_type == "b":

        csv = csv.add_suffix("_b")

        merged_csv_gdf = gdf.merge(
            csv,
            left_on="geo_id_b2020",
            right_on="GEOID_b",
            how="left",
        )

        merged_csv_gdf.drop(columns="GEOID_b", inplace=True)

        pass

    elif census_type == "bg":

        csv = csv.add_suffix("_bg")

        merged_csv_gdf = gdf.merge(
            csv,
            left_on="geo_id_bg2020",
            right_on="GEOID_bg",
            how="left",
        )

        merged_csv_gdf.drop(columns="GEOID_bg", inplace=True)

        pass

    else:
        raise ValueError

    return merged_csv_gdf


def aggregate_by_geo_id(df, geo_layer, agg):
    agg_df = df.groupby(geo_layer).agg(agg).reset_index()

    # Flatten the column MultiIndex after aggregation
    agg_df.columns = [agg_df.columns[0][0]] + [
        "_".join(col).strip() if type(col) is tuple else col
        for col in agg_df.columns[1:]
    ]

    return agg_df
