import sys
import os.path
import psycopg2
import argparse
import requests
from bs4 import BeautifulSoup


def get_player_rows(html_doc, table_id):
    """
    This function takes in a file object and table ID string.
    The file object must be an HTML document that contains a table with
    the provided table ID. The content of the table is contained in <tr> tags
    within a <tbody> tag.
    """

    # Get soup object of the file and then find the table element with table_id
    soup = BeautifulSoup(html_doc, "html.parser")
    tbl_table = soup.find("table", {"id" : table_id})

    # Exit if the table doesn't exist
    if tbl_table is None:
        print("Table ID does not exist. Exiting...")
        sys.exit(1)

    # Get the body and the rows it contains, and then return the rows
    tbl_body = tbl_table.find("tbody")
    tbl_rows = tbl_body.find_all("tr")
    return tbl_rows


def parse_player_rows(player_rows):
    """
    This function accepts a list of row elements in the same format output by
    the get_player_rows function. It adds each column to a player object which
    is an array of fields, and then adds the player object to an array of all
    players, which is what the function returns.
    If the column contains a link (<a> tag), it must be the name field, which
    it processes accordingly by splitting on the comma.
    """

    players = []

    for row in player_rows:

        # For each row, get the data fields, each in a <td> tag and loop through
        cols = row.find_all("td")
        player = []

        for col in cols:
            # Check if this column is a link
            if col.find("a") is not None:

                # If it is a link, extract first initial and last name and add to player object
                item = col.find("a").string
                lst_nm = item.split(",")[0].strip()
                frst_init = item.split(",")[1].strip()
                player.append(lst_nm)
                player.append(frst_init)
            else:
                # If not a link, make sure the column is not empty
                if len(col.string.strip()) > 0:
                    player.append(col.string.strip())
        players.append(player)
    return players


if __name__ == "__main__":
    # Set up command-line arguments
    parser = argparse.ArgumentParser(description='Load data from an HTML file and insert into a PostgreSQL database.')
    parser.add_argument('--file_url', default="https://raw.githubusercontent.com/kruser/interview-developer/master/python/leaderboard.html", dest="file_url",
        help='Choose file to load (default: https://raw.githubusercontent.com/kruser/interview-developer/master/python/leaderboard.html)')
    parser.add_argument('--table_id', default="battingLeaders", dest="table_id",
        help='Enter the ID of the table element within the HTML file (default: battingLeaders)')
    parser.add_argument('--host', default="localhost", dest="host",
        help='Enter the host of the postgreSQL server (default: localhost)')
    parser.add_argument('--dbname', default="batting", dest="dbname",
        help='Choose database to load stats into (default: batting)')
    parser.add_argument('--dbuser', default="postgres", dest="dbuser",
        help='Enter username of the database (default: postgres)')
    parser.add_argument('--dbpass', default="pass", dest="dbpass",
        help='Enter password of the database (default: pass)')

    # Parse and then retrieve command-line arguments
    args = parser.parse_args()

    file_url = args.file_url
    table_id = args.table_id
    host = args.host
    dbname = args.dbname
    dbuser = args.dbuser
    dbpass = args.dbpass


    # Open the file and get the rows, then parse to get player objects
    html_doc = requests.get(file_url).content
    player_rows = get_player_rows(html_doc, table_id)
    player_arr = parse_player_rows(player_rows)

    # Open the connection to the database and get the cursor
    conn = psycopg2.connect(host=host, dbname=dbname, user=dbuser, password=dbpass)
    cur = conn.cursor()

    # Create array of objects for player table, just has ID and name fields
    player_table_arr = [[p[4], p[1], p[2]] for p in player_arr]

    # Insert items into players table
    cur.executemany("INSERT INTO players (player_id, last_nm, frst_nm) VALUES (%s, %s, %s)", player_table_arr)
    conn.commit()

    # Create array of object for stats table, has all stats fields and (player_id, year) as primary key
    stats_table_arr = [[p[4], 2014, p[3], p[5], p[6], p[7], p[8], p[9],
        p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19],
        p[20], p[21], p[22], p[23], p[24], p[25], p[26], p[27], p[28], p[29],
        p[30], p[31], p[32], p[33]] for p in player_arr]

    # Insert items into stats table
    cur.executemany("""INSERT INTO stats (player_id, year, team, pos, games, ab,
    runs, hits, doubles, triples, hr, rbi, bb, so, sb, cs, avg, obp, slg, ops,
    ibb, hbp, sac, sf, tb, xbh, gdp, go, ao, go_ao, np, pa) VALUES (%s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", stats_table_arr)
    conn.commit()

    # Close connection and cursor
    cur.close()
    conn.close()
