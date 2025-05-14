
# Save pyspark DataFrames to CSV
def save_csv(df, filepath):
    """Takes in a pyspark DataFrame and a filepath (including filename).
    Saves the DataFrame to a csv.
    Overwrites any existing CSVs with the same name."""

    df.write.option("header", True) \
        .option("delimiter", ",") \
        .format("csv") \
        .mode("overwrite") \
        .save(filepath)
