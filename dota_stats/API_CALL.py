import requests
import json

#Key 451D543693942BB6C22FC75AC47E3256



def init_api_call():
    match_details = requests.get("https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/V001/?min_players=10&format=JSON&key"
                         "=451D543693942BB6C22FC75AC47E3256")
    request_json = json.loads(match_details.text)
    with open('../results.json', 'w') as outfile:
        for game in request_json["result"]["matches"]:
            request_string = "http://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v001/?match_id={match}&key=451D543693942BB6C22FC75AC47E3256".format(match = str(game["match_id"]))
            player_flag = True
            match_specifics = json.loads(requests.get(request_string).text)
            for player in match_specifics["result"]["players"]:
                if player["leaver_status"] > 1:
                    player_flag = False
                    break
            if player_flag:
                json.dump(match_specifics, outfile,indent=2)

            #TODO Dict with needed info and cleaned matches
    return 0


#def backfill_api():
#TODO Grab init call and compare with current highest match ID from database estimate api calls and add down until backfill is complete