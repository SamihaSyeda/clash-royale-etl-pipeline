from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Enrich the data, preparing to display metrics and graphs
def enrich_data(cleaned_df):
    
    enriched_df = add_league_column(cleaned_df)
    return enriched_df

def add_league_column(df):
    """
     Creates a league column by taking the winner's trophies and calculating which league the battle occurred in.

    Args:
        df (DataFrame): The cleaned dataframe to be enriched.

    Returns:
        DataFrame: A DataFrame with a new column "league" containing which league the battle occurred in.
    """
    
    # Converts league defining function to udf
    league_udf = udf(lambda x:league_definer(x), StringType())

    # Applies league defining function to the winner_starting_trophies column
    # to create a new column "league" with the league that the battle occurred in
    league_df = df.withColumn("league", league_udf(df.winner_starting_trophies))
    
    return league_df

# Takes in the number of trophies that the winner has and returns the appropriate league
def league_definer(trophies):
    """
     Takes the winner's trophies and calculates what league they were battling in.

    Args:
        trophies (int): The number of trophies of the winner.

    Returns:
        Str: A string containing the name of the league which the battle occurred in.
    """
    
    # If winner's trophies meets the league thresholds, set the league to the appropriate one
    if trophies >= 4000 and trophies < 4300:
        league = "Challenger I"
        
    elif trophies >= 4300 and trophies < 4600:
        league = "Challenger II"
        
    elif trophies >= 4600 and trophies < 5000:
        league = "Challenger III"
    
    elif trophies >= 5000 and trophies < 5300:
        league = "Master I"
        
    elif trophies >= 5300 and trophies < 5600:
        league = "Master II"
        
    elif trophies >= 5600 and trophies < 6000:
        league = "Master III"
        
    elif trophies >= 6000 and trophies < 6300:
        league = "Champion"
        
    elif trophies >= 6300 and trophies < 6600:
        league = "Grand Champion"
    
    elif trophies >= 6600 and trophies < 7000:
        league = "Royal Champion"
    
    elif trophies >= 7000:
        league = "Ultimate Champion"
    
    # If the winner's trophies are not equal to or greater than 4000, raise an Error
    else:
        raise "Error: Trophies are outside expected range."
        
    return league
    