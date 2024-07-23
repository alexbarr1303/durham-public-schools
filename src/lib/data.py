import numpy as np
import pandas as pd
import geopandas as gpd


def subset_analytic_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to keep a pre-determined set of columns.
    """
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


def mean_and_round(x: pd.Series) -> float:
    """
    Function to return rounded mean.
    """
    if np.isnan(x.mean()):
        return np.nan
    else:
        return round(x.mean())


def get_parcels(path: str) -> gpd.GeoDataFrame:
    """
    Function to read in parcel data downloaded from Durham Open.
    This can be changed to download the data programatically from
    https://live-durhamnc.opendata.arcgis.com/datasets/da3d194d1e2e4c37afa851b46e29a3f6_0/explore

    The data needs to be downloaded as a shapefile, the path to which can be
    provided in the main script CONFIG.
    """
    parcels_df = gpd.read_file(path)
    return parcels_df


def get_du_est(path: str) -> pd.DataFrame:
    """
    Function to read in the cleaned parcels file from DPS. This should have
    the estimated unit counts per parcel.
    """
    du_df = pd.read_csv(path)
    return du_df


def convert_datetime_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all datetime columns in a pandas DataFrame to string format.

    Parameters:
    df (pandas.DataFrame): The DataFrame to process.

    Returns:
    pandas.DataFrame: The DataFrame with datetime columns converted to strings.
    """
    datetime_cols = df.select_dtypes(
        include=["datetime64[ns]", "datetime64[ns, UTC]"]
    ).columns
    for col in datetime_cols:
        df[col] = df[col].astype(str)
    return df


def add_columns_from_csv(gdf: gpd.GeoDataFrame, csv: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Function to merge the Durham Open parcel data and the DPS parcel date on REID.
    """
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


def fix_geoid_dtypes(series: pd.Series) -> pd.Series:
    """
    Function to convert a column series to str.
    GeoIDs need to be in str instead of integer.
    """
    return series.fillna(-1).astype(int).astype(str)


# Function to safely convert a column to int, fallback to str if it fails
def safe_convert_to_int(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """
    Function to convert column to type integer. If error, convert to str.
    """
    try:
        df[col_name] = df[col_name].astype(int)
    except ValueError:
        df[col_name] = df[col_name].astype(str)
    return df


def add_columns_from_census(
    gdf: gpd.GeoDataFrame, csv: pd.DataFrame, census_type: str = "t"
) -> gpd.GeoDataFrame:
    """
    Function to add census columns.

    Parameters:
    census_type: One of "t", "b", "bg", for tract, block and block group.
    """
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


def aggregate_by_geo_id(
    df: gpd.GeoDataFrame, geo_layer: str, agg: dict
) -> gpd.GeoDataFrame:
    """
    Function to aggregate the geo dataframe by a certain geography encoded in geo_layer.
    """
    agg_df = df.groupby(geo_layer).agg(agg).reset_index()

    # Flatten the column MultiIndex after aggregation
    agg_df.columns = [agg_df.columns[0][0]] + [
        "_".join(col).strip() if type(col) is tuple else col
        for col in agg_df.columns[1:]
    ]

    return agg_df
