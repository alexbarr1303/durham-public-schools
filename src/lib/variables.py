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

    return df
