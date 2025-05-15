from spark_setup import setup_spark
import os
from etl.transform.transform import rename_columns, drop_columns
from etl.transform.transform import filter_gamemode
from etl.transform.transform import keep_battle_date_only, set_types
from etl.transform.transform import enrich_data
import pytest

# Set up a spark session
spark = setup_spark()

# Read in test data as spark DataFrame
# Sample of 200 rows taken from the battle data


@pytest.fixture
def test_df():

    FILE_PATH_TEST_DATA = os.path.join(
        os.path.dirname(__file__),
        "../test_data/battles.csv"
        )
    df = spark.read.csv(FILE_PATH_TEST_DATA, header=True, inferSchema=True)

    return df


@pytest.fixture
def renamed_columns(test_df):
    return rename_columns(test_df)


@pytest.fixture
def dropped_columns(renamed_columns):
    return drop_columns(renamed_columns)


@pytest.fixture
def filtered_gamemode(dropped_columns):
    return filter_gamemode(dropped_columns)


@pytest.fixture
def dropped_time(filtered_gamemode):
    return keep_battle_date_only(filtered_gamemode)


@pytest.fixture
def typed(dropped_time):
    return set_types(dropped_time)


@pytest.fixture
def enriched(typed):
    return enrich_data(typed)


def test_rename_columns(renamed_columns, ):
    # Arrange
    expected_col = ['id', 'battle_time', 'arena_id', 'gamemode_id',
                    'avg_starting_trophies', 'winner_tag',
                    'winner_starting_trophies', 'winner_trophy_change',
                    'winner_crowns', 'winner_king_tower_hp',
                    'winner_princess_towers_hp', 'winner_clan_tag',
                    'winner_clan_badge_id', 'loser_tag',
                    'loser_starting_trophies', 'loser_trophy_change',
                    'loser_crowns', 'loser_king_tower_hp', 'loser_clan_tag',
                    'loser_clan_badge_id', 'loser_princess_towers_hp',
                    'tournament_tag', 'winner_card1_id', 'winner_card1_level',
                    'winner_card2_id', 'winner_card2_level', 'winner_card3_id',
                    'winner_card3_level', 'winner_card4_id',
                    'winner_card4_level', 'winner_card5_id',
                    'winner_card5_level', 'winner_card6_id',
                    'winner_card6_level', 'winner_card7_id',
                    'winner_card7_level', 'winner_card8_id',
                    'winner_card8_level', 'winner_cards_list',
                    'winner_total_card_level', 'winner_troop_count',
                    'winner_structure_count', 'winner_spell_count',
                    'winner_common_count', 'winner_rare_count',
                    'winner_epic_count', 'winner_legendary_count',
                    'winner_elixir_average', 'loser_card1_id',
                    'loser_card1_level',
                    'loser_card2_id', 'loser_card2_level', 'loser_card3_id',
                    'loser_card3_level', 'loser_card4_id', 'loser_card4_level',
                    'loser_card5_id', 'loser_card5_level', 'loser_card6_id',
                    'loser_card6_level', 'loser_card7_id', 'loser_card7_level',
                    'loser_card8_id', 'loser_card8_level', 'loser_cards_list',
                    'loser_total_card_level', 'loser_troop_count',
                    'loser_structure_count', 'loser_spell_count',
                    'loser_common_count', 'loser_rare_count',
                    'loser_epic_count', 'loser_legendary_count',
                    'loser_elixir_average']

    # Assert
    assert (
        renamed_columns.columns == expected_col
    ), "Column names have been renamed incorrectly"


def test_drop_columns(dropped_columns, ):
    # Arrange
    cols_to_drop = ['arena_id', 'winner_king_tower_hp',
                    'winner_princess_towers_hp', 'winner_clan_tag',
                    'winner_clan_badge_id', 'loser_king_tower_hp',
                    'loser_clan_tag', 'loser_clan_badge_id',
                    'loser_princess_towers_hp', 'tournament_tag',
                    'winner_troop_count', 'winner_structure_count',
                    'winner_spell_count', 'winner_common_count',
                    'winner_rare_count', 'winner_epic_count',
                    'winner_legendary_count', 'loser_troop_count',
                    'loser_structure_count', 'loser_spell_count',
                    'loser_common_count', 'loser_rare_count',
                    'loser_epic_count', 'loser_legendary_count']

    col_checker = check_columns(cols_to_drop, dropped_columns)

    # Assert
    assert (col_checker), "Columns have been dropped incorrectly."


def check_columns(cols_to_drop, df):
    for col in cols_to_drop:
        if col in df.columns:
            return False

        return True


def test_filter_gamemode(filtered_gamemode, ):

    # Arrange
    gamemodes = filtered_gamemode.select("gamemode_id").distinct()
    gamemodes_list = gamemodes.select(
        "gamemode_id").rdd.map(lambda x: x[0]).collect()

    # Assert
    assert (
        gamemodes_list == [72000006]
    ), "Gamemode has been filtered incorrectly."


def test_battle_time_dropped(dropped_time, ):

    # Assert
    assert (
        "battle_time" not in dropped_time.columns
    ), "battle_time column was not dropped"


def test_battle_date_added(dropped_time, ):

    # Assert
    assert (
        "battle_date" in dropped_time.columns
    ), "battle_date column was not added"


def test_type_battle_date(typed, ):

    # Arrange
    expected_type = "date"
    actual_type = get_dtype(typed, "battle_date")

    # Assert
    assert (
        actual_type == expected_type
    ), "battle_date has not been set to datetime."


def get_dtype(df, colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]


def test_enrich_data_added_league_column(enriched, ):

    # Assert
    assert (
        "league" in enriched.columns
    ), "league column was not added"
