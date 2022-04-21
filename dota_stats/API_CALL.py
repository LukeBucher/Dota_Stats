import requests
import json
import sqlite3 as sql
from sqlite3 import Error

#Key 451D543693942BB6C22FC75AC47E3256

def init_api_call(): #Start baseline match
    cleaned_match = {}
    matches = requests.get("https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/?min_players=10&format=JSON&key"
                         "=451D543693942BB6C22FC75AC47E3256")
    request_json = json.loads(matches.text)
    for game in request_json["result"]["matches"]:
        request_string = "http://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v001/?match_id={match}&key=451D543693942BB6C22FC75AC47E3256".format(match=str(game["match_id"]))
        player_flag = True
        match_details = json.loads(requests.get(request_string).text)
        match_details = match_details["result"]
        for player in match_details["players"]:
            if player["leaver_status"] > 1:
                player_flag = False
                break
        if player_flag:
            #Generate Dictionary from API_CALL
            print(match_details)
            cleaned_match[match_details['match_id']] = {
                'start_date': match_details['start_time'],
                'duration': match_details['duration'],
                'radiant_hero_1_id': match_details['players'][0]['hero_id'],
                'radiant_hero_2_id': match_details['players'][1]['hero_id'],
                'radiant_hero_3_id': match_details['players'][2]['hero_id'],
                'radiant_hero_4_id': match_details['players'][3]['hero_id'],
                'radiant_hero_5_id': match_details['players'][4]['hero_id'],
                'dire_hero_1_id': match_details['players'][5]['hero_id'],
                'dire_hero_2_id': match_details['players'][6]['hero_id'],
                'dire_hero_3_id': match_details['players'][7]['hero_id'],
                'dire_hero_4_id': match_details['players'][8]['hero_id'],
                'dire_hero_5_id': match_details['players'][9]['hero_id'],
                'winning_team': match_details['radiant_win']

            }
    return list(cleaned_match.items())



#TODO Grab init call and compare with current highest match ID from database estimate api calls and add down until backfill is complete


def back_fill(last_match_ID,cursor):  #Test key: 6531080579
    matches = requests.get("https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/?min_players=10&start_at_match_id =%s&format=JSON&key"
                         "=451D543693942BB6C22FC75AC47E3256" %(last_match_ID))
    request_json = json.loads(matches.text)
    return 0

    #If no database seen backfill from top down to current "bedrock" match
    #If database seen backfill until latest match to complete data set


def database_connection(database):  # Returns database cursor
    try:
        connection = sql.connect(database)
        cursor = connection.cursor()
        return cursor
    except Error as e:
        print(e)


def get_last_match_ID(cursor):
    cursor.execute("Select * FROM matches ORDER BY date_start DESC LIMIT 1 ")
    return cursor.fetchall()

def close_connection(cursor):
    cursor.connection.commit()
    cursor.connection.close()

def main():
    database_URI = "matches.db"
    db = database_connection(database_URI)
    create_match_table = "CREATE TABLE IF NOT EXISTS matches(match_id int PRIMARY KEY, date_start INTEGER, duration " \
                         "INTEGER, radiant_hero_1_id INTEGER NOT NULL DEFAULT '0',radiant_hero_2_id INTEGER NOT NULL " \
                         "DEFAULT '0',radiant_hero_3_id INTEGER NOT NULL DEFAULT '0',radiant_hero_4_id INTEGER NOT " \
                         "NULL DEFAULT '0',radiant_hero_5_id INTEGER NOT NULL DEFAULT '0',dire_hero_1_id INTEGER NOT " \
                         "NULL DEFAULT '0',dire_hero_2_id INTEGER NOT NULL DEFAULT '0',dire_hero_3_id INTEGER NOT " \
                         "NULL DEFAULT '0',dire_hero_4_id INTEGER NOT NULL DEFAULT '0',dire_hero_5_id INTEGER NOT " \
                         "NULL DEFAULT '0',winning_team BOOL) "
    if db is not None:
        db.execute(create_match_table)
        test_matches = init_api_call()
        for match in test_matches:
            inserted_values = (list(match[1].values()))
            inserted_values.insert(0, match[0])
            db.execute("INSERT INTO matches (match_id,date_start,duration,radiant_hero_1_id,radiant_hero_2_id,"
                       "radiant_hero_3_id,radiant_hero_4_id,radiant_hero_5_id,dire_hero_1_id,dire_hero_2_id,"
                       "dire_hero_3_id,dire_hero_4_id,dire_hero_5_id,winning_team) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", inserted_values)
        close_connection(db)


main()
cur = database_connection("matches.db")
print(get_last_match_ID(cur))
back_fill(get_last_match_ID(cur),cur)

