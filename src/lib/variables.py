def process_data(df):
    # Calculate unit value and add as a new column
    df["unit_val"] = df["TOTAL_PROP_VALUE"] / df["du_est_final"]

    # Calculate quartile thresholds for unit_val in df
    quartiles = df["unit_val"].quantile([0, 0.25, 0.5, 0.75, 1])

    # Function to assign quartile category
    def assign_quartile(unit_val):
        if unit_val >= quartiles.iloc[3]:  # Fourth quartile
            return 4
        elif unit_val >= quartiles.iloc[2]:  # Third quartile
            return 3
        elif unit_val >= quartiles.iloc[1]:  # Second quartile
            return 2
        else:  # First quartile
            return 1

    # Apply function to create unit_val_cat column
    df["unit_val_cat"] = df["unit_val"].apply(assign_quartile)

    # Create subsets based on designation
    df_single = df[df["designation"] == "single"].copy()
    df_multi = df[df["designation"] == "multi"].copy()

    # Calculate quartile thresholds for unit_val in df_single
    quartiles_single = df_single["unit_val"].quantile([0, 0.25, 0.5, 0.75, 1])

    # Function to assign quartile category for df_single
    def assign_quartile_single(unit_val):
        if unit_val >= quartiles_single.iloc[3]:  # Fourth quartile
            return 4
        elif unit_val >= quartiles_single.iloc[2]:  # Third quartile
            return 3
        elif unit_val >= quartiles_single.iloc[1]:  # Second quartile
            return 2
        else:  # First quartile
            return 1

    # Apply function to create unit_val_cat_single column in df_single
    df_single["unit_val_cat_single"] = df_single["unit_val"].apply(
        assign_quartile_single
    )

    # Calculate quartile thresholds for unit_val in df_multi
    quartiles_multi = df_multi["unit_val"].quantile([0, 0.25, 0.5, 0.75, 1])

    # Function to assign quartile category for df_multi
    def assign_quartile_multi(unit_val):
        if unit_val >= quartiles_multi.iloc[3]:  # Fourth quartile
            return 4
        elif unit_val >= quartiles_multi.iloc[2]:  # Third quartile
            return 3
        elif unit_val >= quartiles_multi.iloc[1]:  # Second quartile
            return 2
        else:  # First quartile
            return 1

    # Apply function to create unit_val_cat_multi column in df_multi
    df_multi["unit_val_cat_multi"] = df_multi["unit_val"].apply(assign_quartile_multi)

    # Merge unit_val_cat_single column into the original dataframe
    df = df.merge(df_single[["REID", "unit_val_cat_single"]], on="REID", how="left")

    # Merge unit_val_cat_multi column into the original dataframe
    df = df.merge(df_multi[["REID", "unit_val_cat_multi"]], on="REID", how="left")

    # Group by pu_2324_848 (Planning Unit)
    pu_avg = df.groupby('pu_2324_848').agg({
        'unit_val': 'mean',
        'unit_val_cat_single': lambda x: round(x.mean()) if not np.isnan(x.mean()) else np.nan,
        'unit_val_cat_multi': lambda x: round(x.mean()) if not np.isnan(x.mean()) else np.nan
    }).reset_index()

    # Rename columns for PUs
    pu_avg.columns = ['pu_2324_848', 'unit_val_avg_pu2020', 'unit_val_cat_single_avg_pu2020', 'unit_val_cat_multi_avg_pu2020']

    # Merge PU averages back to df
    df = pd.merge(df, pu_avg, on='pu_2324_848', how='left')

   # Group by geo_id_b2020 (block)
    block_avg = df.groupby('geo_id_b2020').agg({
        'unit_val': 'mean',
        'unit_val_cat_single': lambda x: round(x.mean()) if not np.isnan(x.mean()) else np.nan,
        'unit_val_cat_multi': lambda x: round(x.mean()) if not np.isnan(x.mean()) else np.nan
    }).reset_index()

    # Rename columns for blocks
    block_avg.columns = ['geo_id_b2020', 'unit_val_avg_b2020', 'unit_val_cat_single_avg_b2020', 'unit_val_cat_multi_avg_b2020']

    # Merge block averages back to df
    df = pd.merge(df, block_avg, on='geo_id_b2020', how='left')

    # Group by geo_id_bg2020 (block group)
    block_group_avg = df.groupby('geo_id_bg2020').agg({
        'unit_val': 'mean',
        'unit_val_cat_single': lambda x: round(x.mean()) if not np.isnan(x.mean()) else np.nan,
        'unit_val_cat_multi': lambda x: round(x.mean()) if not np.isnan(x.mean()) else np.nan
    }).reset_index()

    # Rename columns for block groups
    block_group_avg.columns = ['geo_id_bg2020', 'unit_val_avg_bg2020', 'unit_val_cat_single_avg_bg2020', 'unit_val_cat_multi_avg_bg2020']

    # Merge block group averages back to df
    df = pd.merge(df, block_group_avg, on='geo_id_bg2020', how='left')

    # Group by geo_id_t2020 (tract)
    tract_avg = df.groupby('geo_id_t2020').agg({
        'unit_val': 'mean',
        'unit_val_cat_single': lambda x: round(x.mean()) if not np.isnan(x.mean()) else np.nan,
        'unit_val_cat_multi': lambda x: round(x.mean()) if not np.isnan(x.mean()) else np.nan
    }).reset_index()

    # Rename columns for tracts
    tract_avg.columns = ['geo_id_t2020', 'unit_val_avg_t2020', 'unit_val_cat_single_avg_t2020', 'unit_val_cat_multi_avg_t2020']

    # Merge tract averages back to df
    df = pd.merge(df, tract_avg, on='geo_id_t2020', how='left')

    return df



