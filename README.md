# Clash Royale S18 (Logmas): Ladder Data ETL Pipeline
Repository containing an ETL pipeline which cleans and transforms a CSV on Clash Royale S18 ladder data.

## ETL Setup

1. Navigate to the project directory in the terminal.
2. Run `python3 -m venv .venv` to create a virtual environment.
3. Run `source .venv/bin/activate` if on Mac or `source .venv/Scripts/activate` if on Windows to activate the virtual environment.
4. Run `pip install -r requirements-setup.txt` to install setup requirements into your virtual environment.
5. Run the setup file with `pip install -e .`
6. Run `run_etl dev` to run the ETL pipeline in the dev environment.

**Note:** This dataset has 7,343,747 rows. As such, this pipeline uses pyspark to extract, transform, and load data in order to make processes more efficient.
Due to pyspark compatibility issues, you must have installed and added to your environment path, **Java JDK 8 and Python 3.11.8** for this pipeline to run.

Due to the size of the dataset, I was unable to upload it to github. The dataset can be accessed from kaggle [here](https://www.kaggle.com/datasets/bwandowando/clash-royale-season-18-dec-0320-dataset).

There is a kaggle API but it seems that if the connection is disrupted, the download would need to be restarted and for a large file, this is a hassle. Nevertheless, for the sake of automation, the kaggle API would be a good thing to implement in the pipeline in future, as part of the extract process, so the pipeline is fully automated with no need for a human to manually download the dataset.
