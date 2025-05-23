import os
import sys
from config.env_config import setup_env
from spark_setup import setup_spark
from etl.extract.extract import extract_data
from etl.transform.transform import clean_transform_data
from etl.load.load import save_csv


def main():
    # Run environment setup
    run_env_setup()

    # Run spark session setup
    spark = setup_spark()

    # Extract battle data
    print("Extracting data...")

    FILE_PATH_BATTLES = os.path.join(
        os.path.dirname(__file__),
        "../data/raw/Battles_12072020_to_12262020.csv"
        )
    df = extract_data(spark, FILE_PATH_BATTLES)

    print("Data extracted successfully.")

    print("Cleaning, transforming and enriching data...")
    enriched_df = clean_transform_data(df)
    print("Data has been enriched (with cheese).")

    print("Loading data into CSV...")
    FILE_PATH_ENRICHED = os.path.join(
        os.path.dirname(__file__), "../data/processed/enriched_data.csv"
        )

    save_csv(enriched_df, FILE_PATH_ENRICHED)
    print("Data loaded yayy...")

    print(
        f"ETL pipeline run successfully in "
        f'{os.getenv("ENV", "error")} environment!'
    )


def run_env_setup():
    print("Setting up environment...")
    setup_env(sys.argv)
    print("Environment setup complete.")


if __name__ == "__main__":
    main()
