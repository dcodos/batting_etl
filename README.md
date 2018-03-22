# MLB Python ETL and Database Load

## Install dependencies
To install dependencies, run `pip install -r requirements.txt`

**Note: Python 3 is required**

## Table Setup
There are two tables where data will be stored, *players* and *stats*.
The *players* table stores player IDs and the names of players, while the *stats* table stores all of the statistics found in the HTML file. This allows for multiple years of data to be stored in *stats* without storing the player name redundantly.

The DDL to create these two tables is in `stats_tables.sql`

## Running the app
To run the code with the default parameters, simply run `python app.py`.

The parameters and their defaults are as follows:

|Argument Name|Command|Default|
|--------|--------|-------|
|File URL|--file_url|https://raw.githubusercontent.com/kruser/interview-developer/master/python/leaderboard.html
|Table ID|--table_id|battingLeaders|
|Host|--host|localhost|
|DB Name|--dbname|batting|
|DB Username|--dbuser|postgres|
|DB Password|--dbpass|pass|

## wOBA Analysis
To get a list of all of the players, sorted by 2014 wOBA, run the script in `woba.sql`

This query uses the wOBA weights from Fangraphs found here: https://www.fangraphs.com/guts.aspx?type=cn
