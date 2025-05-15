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

I could also upload the dataset to a database like Postgres and extract the data from there via the pipeline.

## User Stories

1. As a player, I want to know what the top 10 most widely used cards were so I can see what was popular in the logmas meta.
2. As a player, I want to know what the top 10 most used win conditions were so I can see what type of decks were doing well during logmas.
3. As a player, I want to be able to filter the most used cards and most used win conditions by trophy range (league) so I know which cards were most used in my ladder league.
4. As a game developer, I want to know if there is an increase in usage of buffed cards so I can see whether card buffs are effective in bringing up card usage.
5. As a game developer, I want to know if there is a decrease in usage of nerfed cards so I can see whether card nerfs are effective in bringing down card usage of overly popular cards.
6. As a Clash Royale Fan, I want to know how many unique players played ladder in this logmas season so I can see if this season truly was the best season of Clash Royale (I mean we had nowhere else to go, really, we were stuck inside).
7. As a Clash Royale Fan I want to know what the total number of crowns won in this season was just because it is a really cool stat


## User Story 1 and 3:
As a player, I want to know what the top 10 most widely used cards were so I can see what was popular in the logmas meta.

### Definition of Done

- [ ] Take all the battles and group them into different leagues: Challenger, Master, Champion, Grand Champion, Royal Champion, Ultimate Champion
- [ ] Gather the card counts for each card_id, go through every card_id in card_id csv and count how many of them in each arena (across different card_number columns)
- [ ] Store the names of the top 10 highest used cards
- [ ] Streamlit bar chart for usage counts of top 10 cards with filtering by league
- [ ] All functions are unit tested and pass the tests

## User Story 2 and 3:
As a player, I want to know what the top 10 most used win conditions were so I can see what type of decks were doing well during logmas.

### Definition of Done

- [ ] Take all the battles and group them into different leagues: Challenger, Master, Champion, Grand Champion, Royal Champion, Ultimate Champion
- [ ] Gather the card counts for each card_id, go through every card_id in card_id csv and count how many of them in each arena (across different card_number columns)
- [ ] Filter the counts for only win-cons
- [ ] Store the names of the top 10 win-cons
- [ ] Streamlit bar chart for usage counts of top 10 most used win conditions with filtering by league
- [ ] All functions are unit tested and pass the tests

## Useful Analysis To-Be Completed in Near Future: (User Stories 4 and 5)
4. As a game developer, I want to know if there is an increase in usage of buffed cards so I can see whether card buffs are effective in bringing up card usage.
5. As a game developer, I want to know if there is a decrease in usage of nerfed cards so I can see whether card nerfs are effective in bringing down card usage of overly popular cards.

## User Story 6 and 3:
6. As a Clash Royale Fan, I want to know how many unique players played ladder in this logmas season so I can see if this season truly was the best season of Clash Royale (I mean we had nowhere else to go, really, we were stuck inside). As a dev I want to know if our logmas promotions and media worked in increasing game engagement.

### Definition of Done

- [ ] Take all the battles and group them into different leagues: Challenger, Master, Champion, Grand Champion, Royal Champion, Ultimate Champion
- [ ] Join the loser tags column vertically with the winner tags column to get all the player tags in one column
- [ ] Filter the player tags for unique tags using distinct()
- [ ] Count the distinct number of players.
- [ ] Streamlit metric for number of players, filtered by league.
- [ ] All functions are unit tested and pass the tests

## User Story 7 and 3:
7. As a Clash Royale Fan I want to know what the total number of crowns won in this season was just because it is a really cool stat. (on a serious note, as a dev, it helps us understand what amount we should set the next community crown challenge to so that it is not too hard for the community to achieve, nor too easy).

With this data, the following can also possibly be answered:

- Which cards are the strongest? The weakest? (in terms of win-rate)
- Which win-con is the most winning?
- When 2 opposing players are using maxed decks, which win-con is the most winning?
- Most widely used cards? Win-Cons?
- Do we see an increase in usage of cards that are level boosted for the season?
- How effective are balance changes? 
  - Do we see an increase in usage of buffed cards? 
  - Do we see a decrease in usage of nerfed cards?
  - In general, how did reworks affect card usage?
- Which cards are always with a specific win-con?


## Function and Flow of ETL Planning Flowchart here: [Lucid Chart](https://lucid.app/lucidchart/42f58c41-dad1-47f1-a884-5855c2e8cbb8/edit?viewport_loc=41%2C-359%2C3068%2C1341%2C0_0&invitationId=inv_a1117dca-caff-43d5-a694-9fbec5e866b9)

