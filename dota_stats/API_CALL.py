import requests
import json
import sqlite3 as sql
from sqlite3 import Error
#Key 451D543693942BB6C22FC75AC47E3256



def init_api_call():
    matches = requests.get("https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/?min_players=10&format=JSON&key"
                         "=451D543693942BB6C22FC75AC47E3256")
    request_json = json.loads(matches.text)
    for game in request_json["result"]["matches"]:
        request_string = "http://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v001/?match_id={match}&key=451D543693942BB6C22FC75AC47E3256".format(match = str(game["match_id"]))
        player_flag = True
        match_details = json.loads(requests.get(request_string).text)
        for player in match_details["result"]["players"]:
            if player["leaver_status"] > 1:
                player_flag = False
                break
        if player_flag:
            print(match_details)

        #TODO Dict with needed info and cleaned matches
    return 0


#def backfill_api():
#TODO Grab init call and compare with current highest match ID from database estimate api calls and add down until backfill is complete


def back_fill(last_match_ID,cursor):
    return 0

    #If no database seen backfill from top down to current "bedrock" match
    #If database seen backfill until latest match to complete data set

def database_connection(database):
    try:
        connection = sql.connect(database)
        cursor = connection.cursor()
        return cursor
    except Error as e:
        print(e)


def get_last_match_ID(cursor):
    cursor.execute("Select * FROM matches ORDER BY date_start DESC LIMIT 1 ")

def close_connection(cursor):
    cursor.connection.commit()
    cursor.connection.close()

def main():
    database_URI = "matches.db"
    db = database_connection(database_URI)
    create_match_table = "CREATE TABLE IF NOT EXISTS matches(" \
                         "match_id int PRIMARY KEY," \
                         "date_start INTEGER, " \
                         "duration INTEGER," \
                         " `radiant_hero_1_id` smallint unsigned NOT NULL DEFAULT '0',\
                        `radiant_hero_2_id` INTEGER NOT NULL DEFAULT '0', \
                        `radiant_hero_3_id` INTEGER NOT NULL DEFAULT '0',\
                        `radiant_hero_4_id` INTEGER NOT NULL DEFAULT '0',\
                        `radiant_hero_5_id` INTEGER NOT NULL DEFAULT '0',\
                        `dire_hero_1_id` INTEGER NOT NULL DEFAULT '0',\
                        `dire_hero_2_id` INTEGER NOT NULL DEFAULT '0',\
                        `dire_hero_3_id` INTEGER NOT NULL DEFAULT '0',\
                        `dire_hero_4_id` INTEGER NOT NULL DEFAULT '0',\
                        `dire_hero_5_id` INTEGER NOT NULL DEFAULT '0',\
                        'winning_team BOOL')"
    if db is not None:
        db.execute(create_match_table)
        close_connection(db)



init_api_call()
