from pyspark.sql.functions import udf, round
from pyspark.sql.types import StringType, DateType, IntegerType, FloatType
from enrich import enrich_data

# Cleans and Transforms the data and returns the cleaned and transformed dataframe
def clean_transform_data(df):
    """
     Cleans and Transforms a DataFrame by renaming columns,
     dropping unnecessary columns,
     filtering out records that are not ladder matches,
     filtering out records where the winner has less than 4k trophies,
     Removing the time from the battleTime column but keeping the year,
     Setting columns to their appropriate types.

    Args:
        df (DataFrame): The DataFrame to be cleaned and transformed.

    Returns:
        DataFrame: The cleaned and transformed DataFrame.
    """
    
    # Runs the cleaning and transformation functions and store as a new DataFrame
    cleaned_df = rename_columns(df)
    cleaned_df = drop_columns(cleaned_df)
    cleaned_df = filter_gamemode(cleaned_df)
    cleaned_df = filter_trophies(cleaned_df)
    cleaned_df = keep_battle_date_only(cleaned_df)
    cleaned_df = set_types(cleaned_df)
    
    enriched_df = enrich_data(cleaned_df)
    
    return enriched_df

# Renames the columns of a DataFrame to neater names and returns this as a new DataFrame
def rename_columns(df):
    """
     Renames the columns of a DataFrame to neater names.

    Args:
        df (DataFrame): The DataFrame whose columns will be renamed.

    Returns:
        DataFrame: The DataFrame with the columns renamed.
    """
    
    # New column names
    new_cols = ['id','battle_time','arena_id','gamemode_id','avg_starting_trophies','winner_tag',
    'winner_starting_trophies','winner_trophy_change','winner_crowns','winner_king_tower_hp',
    'winner_princess_towers_hp','winner_clan_tag','winner_clan_badge_id','loser_tag',
    'loser_starting_trophies','loser_trophy_change','loser_crowns','loser_king_tower_hp',
    'loser_clan_tag','loser_clan_badge_id','loser_princess_towers_hp','tournament_tag',
    'winner_card1_id','winner_card1_level','winner_card2_id','winner_card2_level',
    'winner_card3_id','winner_card3_level','winner_card4_id','winner_card4_level',
    'winner_card5_id','winner_card5_level','winner_card6_id','winner_card6_level',
    'winner_card7_id','winner_card7_level','winner_card8_id','winner_card8_level',
    'winner_cards_list','winner_total_card_level','winner_troop_count','winner_structure_count',
    'winner_spell_count','winner_common_count','winner_rare_count','winner_epic_count','winner_legendary_count',
    'winner_elixir_average','loser_card1_id','loser_card1_level','loser_card2_id','loser_card2_level',
    'loser_card3_id','loser_card3_level','loser_card4_id','loser_card4_level','loser_card5_id',
    'loser_card5_level','loser_card6_id','loser_card6_level','loser_card7_id','loser_card7_level',
    'loser_card8_id','loser_card8_level','loser_cards_list','loser_total_card_level','loser_troop_count',
    'loser_structure_count','loser_spell_count','loser_common_count','loser_rare_count',
    'loser_epic_count','loser_legendary_count','loser_elixir_average']

    # Changes all the column names to the neater names in the new_cols list
    renamed_df = df.toDF(*new_cols)
    
    return renamed_df

# Filters for the records that are ladder matches and returns this filtered DataFrame
def filter_gamemode(df):
    """
     Filters the records of a DataFrame to select only ladder matches.

    Args:
        df (DataFrame): The DataFrame whose records will be filtered.

    Returns:
        DataFrame: The DataFrame containing only data for ladder matches.
    """
    # Filters for the battles that had gamemode_ID 72000006 which are ladder matches
    filtered_data = df.filter(df["gamemode_id"] == 72000006)
    
    return filtered_data

# Drops unnecessary columns and returns this cleaned DataFrame
def drop_columns(df):
    """
     Drops the unnecessary columns of a DataFrame.

    Args:
        df (DataFrame): The DataFrame whose columns will be selectively dropped.

    Returns:
        DataFrame: The DataFrame containing only necessary columns.
    """
    
    # Drops the specified columns from the DataFrame, storing the new DataFrame
    cleaned_df = df.drop('arena_id','winner_king_tower_hp','winner_princess_towers_hp',
                        'winner_clan_tag','winner_clan_badge_id','loser_king_tower_hp',
                        'loser_clan_tag','loser_clan_badge_id','loser_princess_towers_hp',
                        'tournament_tag','winner_troop_count','winner_structure_count',
                        'winner_spell_count','winner_common_count','winner_rare_count',
                        'winner_epic_count','winner_legendary_count','loser_troop_count',
                        'loser_structure_count','loser_spell_count','loser_common_count',
                        'loser_rare_count','loser_epic_count','loser_legendary_count')

    return cleaned_df

def filter_trophies(df):
    """
     Filters the records of a DataFrame to select only ranked league matches.

    Args:
        df (DataFrame): The DataFrame whose records will be filtered.

    Returns:
        DataFrame: The DataFrame containing only data for the 4k+ trophy range.
    """
    
    # Filters for the battles where the winner had more than or equal to 4k trophies (ranked league games)
    filtered_data = df.filter(df["winner_starting_trophies"] >= 4000)
    
    return filtered_data

# Splices the first 10 characters of a string and returns the spliced string
def first_ten_chars(date_time):
    """
     Splices the first 10 characters of a string.

    Args:
        date_time (Str): The string to be spliced.

    Returns:
        Str: The spliced string.
    """
    return date_time[: 10]

def keep_battle_date_only(df):
    """
     Creates a new column battle_date with only the battle date (removing the time).

    Args:
        df (DataFrame): The DataFrame to be modified.

    Returns:
        DataFrame: The DataFrame containing the column battle_date with just the date,
                and with the original battle_time column dropped.
    """
    
    # Converts splicing function to udf
    first_ten_udf = udf(lambda x:first_ten_chars(x), StringType())

    # Converts battle_time column to a string type
    df = df.withColumn("battle_time", df.battle_time.cast(StringType()))

    # Applies splicing function to the battle_time column to create a new column "battle_date" with just the date
    cleaned_date_df = df.withColumn("battle_date", first_ten_udf(df.battle_time))
    
    # Drops the old battle_time column
    cleaned_date_df = cleaned_date_df.drop("battle_time")
    
    return cleaned_date_df

# Converts all columns to their appropriate data types
def set_types(df):
    """
     Sets all the columns of the dataframe to its appropriate data types.

    Args:
        df (DataFrame): The DataFrame whose columns will be converted to the correct type.

    Returns:
        DataFrame: The DataFrame with the columns set to its correct data type.
    """
    
    # Set the appropriate type for all columns one by one and round floats where necessary
    df = df.withColumn("id", df.id.cast(IntegerType()))
    df = df.withColumn("gamemode_id", df.gamemode_id.cast(IntegerType()))
    df = df.withColumn("avg_starting_trophies", df.avg_starting_trophies.cast(FloatType()))
    df = df.withColumn("winner_tag", df.winner_tag.cast(StringType()))
    df = df.withColumn("winner_starting_trophies", df.winner_starting_trophies.cast(IntegerType()))
    df = df.withColumn("winner_trophy_change", df.winner_trophy_change.cast(IntegerType()))
    df = df.withColumn("winner_crowns", df.winner_crowns.cast(IntegerType()))
    df = df.withColumn("loser_tag", df.loser_tag.cast(StringType()))
    df = df.withColumn("loser_starting_trophies", df.loser_starting_trophies.cast(IntegerType()))
    df = df.withColumn("loser_trophy_change", df.loser_trophy_change.cast(IntegerType()))
    df = df.withColumn("loser_crowns", df.loser_crowns.cast(IntegerType()))
    df = df.withColumn("winner_card1_id", df.winner_card1_id.cast(IntegerType()))
    df = df.withColumn("winner_card1_level", df.winner_card1_level.cast(IntegerType()))
    df = df.withColumn("winner_card2_id", df.winner_card2_id.cast(IntegerType()))
    df = df.withColumn("winner_card2_level", df.winner_card2_level.cast(IntegerType()))
    df = df.withColumn("winner_card3_id", df.winner_card3_id.cast(IntegerType()))
    df = df.withColumn("winner_card3_level", df.winner_card3_level.cast(IntegerType()))
    df = df.withColumn("winner_card4_id", df.winner_card4_id.cast(IntegerType()))
    df = df.withColumn("winner_card4_level", df.winner_card4_level.cast(IntegerType()))
    df = df.withColumn("winner_card5_id", df.winner_card5_id.cast(IntegerType()))
    df = df.withColumn("winner_card5_level", df.winner_card5_level.cast(IntegerType()))
    df = df.withColumn("winner_card6_id", df.winner_card6_id.cast(IntegerType()))
    df = df.withColumn("winner_card6_level", df.winner_card6_level.cast(IntegerType()))
    df = df.withColumn("winner_card7_id", df.winner_card7_id.cast(IntegerType()))
    df = df.withColumn("winner_card7_level", df.winner_card7_level.cast(IntegerType()))
    df = df.withColumn("winner_card8_id", df.winner_card8_id.cast(IntegerType()))
    df = df.withColumn("winner_card8_level", df.winner_card8_level.cast(IntegerType()))
    df = df.withColumn("winner_total_card_level", df.winner_total_card_level.cast(IntegerType()))
    df = df.withColumn("winner_elixir_average", df.winner_elixir_average.cast(FloatType()))
    df = df.withColumn("winner_elixir_average", round(df.winner_elixir_average, 2))
    df = df.withColumn("loser_card1_id", df.loser_card1_id.cast(IntegerType()))
    df = df.withColumn("loser_card1_level", df.loser_card1_level.cast(IntegerType()))
    df = df.withColumn("loser_card2_id", df.loser_card2_id.cast(IntegerType()))
    df = df.withColumn("loser_card2_level", df.loser_card2_level.cast(IntegerType()))
    df = df.withColumn("loser_card3_id", df.loser_card3_id.cast(IntegerType()))
    df = df.withColumn("loser_card3_level", df.loser_card3_level.cast(IntegerType()))
    df = df.withColumn("loser_card4_id", df.loser_card4_id.cast(IntegerType()))
    df = df.withColumn("loser_card4_level", df.loser_card4_level.cast(IntegerType()))
    df = df.withColumn("loser_card5_id", df.loser_card5_id.cast(IntegerType()))
    df = df.withColumn("loser_card5_level", df.loser_card5_level.cast(IntegerType()))
    df = df.withColumn("loser_card6_id", df.loser_card6_id.cast(IntegerType()))
    df = df.withColumn("loser_card6_level", df.loser_card6_level.cast(IntegerType()))
    df = df.withColumn("loser_card7_id", df.loser_card7_id.cast(IntegerType()))
    df = df.withColumn("loser_card7_level", df.loser_card7_level.cast(IntegerType()))
    df = df.withColumn("loser_card8_id", df.loser_card8_id.cast(IntegerType()))
    df = df.withColumn("loser_card8_level", df.loser_card8_level.cast(IntegerType()))
    df = df.withColumn("loser_total_card_level", df.loser_total_card_level.cast(IntegerType()))
    df = df.withColumn("loser_elixir_average", df.loser_elixir_average.cast(FloatType()))
    df = df.withColumn("loser_elixir_average", round(df.loser_elixir_average, 2))
    df = df.withColumn("battle_date", df.battle_date.cast(DateType()))
    
    return df