# Read in CSV file using spark
def extract_data(spark, filepath):
    """Takes a filepath to a CSV file, with a header.
    Returns a pyspark DataFrame of that data."""

    try:
        # Reading in the csv with the data
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        return df

    except Exception:
        raise Exception(f"Failed to load CSV file: {filepath}")
