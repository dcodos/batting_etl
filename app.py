from bs4 import BeautifulSoup
import psycopg2
import os.path
import sys


def get_player_rows(file_object, table_id):
    soup = BeautifulSoup(file_object, "html.parser")
    tbl_table = soup.find("table", {"id" : table_id})
    if tbl_table is None:
        print("Table ID does not exist. Exiting...")
        sys.exit(1)
    tbl_body = tbl_table.find("tbody")
    tbl_rows = tbl_body.find_all("tr")
    return tbl_rows


def parse_player_rows(player_rows):
    players = []
    for row in player_rows:
        cols = row.find_all("td")
        player = []
        for col in cols:
            if col.find("a") is not None:
                item = col.find("a").string
                lst_nm = item.split(",")[0].strip()
                frst_init = item.split(",")[1].strip()
                player.append(lst_nm)
                player.append(frst_init)
            else:
                if len(col.string.strip()) > 0:
                    player.append(col.string.strip())
        players.append(player)
    return players


if __name__ == "__main__":

    file_path = "leaderboard.html"
    if not os.path.isfile(file_path):
        print("File not found. Exiting...")
        sys.exit(1)

    f = open(file_path)
    player_rows = get_player_rows(f, "battingLeaders")
    player_arr = parse_player_rows(player_rows)

    conn_string = "host='localhost' dbname='batting' user='postgres' password='pass'"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    player_table_arr = [[p[4], p[1], p[2]] for p in player_arr]
    cur.executemany("INSERT INTO players (player_id, last_nm, frst_nm) VALUES (%s, %s, %s)", player_table_arr)
    conn.commit()

    stats_table_arr = [[p[4], 2014, p[3], p[5], p[6], p[7], p[8], p[9],
        p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19],
        p[20], p[21], p[22], p[23], p[24], p[25], p[26], p[27], p[28], p[29],
        p[30], p[31], p[32], p[33]] for p in player_arr]

    cur.executemany("""INSERT INTO stats (player_id, year, team, pos, games, ab,
    runs, hits, doubles, triples, hr, rbi, bb, so, sb, cs, avg, obp, slg, ops,
    ibb, hbp, sac, sf, tb, xbh, gdp, go, ao, go_ao, np, pa) VALUES (%s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", stats_table_arr)
    conn.commit()

    cur.close()
    conn.close()
