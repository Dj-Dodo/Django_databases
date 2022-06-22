from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.shortcuts import render
import psycopg2 as psy
import environ

# Create your views here.
#request -> response


env = environ.Env()
environ.Env.read_env()

try:
    connection = psy.connect(
        user = env('DBUSER'),
        password = env('DBPASS'),
        host = "147.175.150.216",
        port = "5432",
        database = "dota2"
    )
    
    cursor = connection.cursor()
    

except(psy.Error):
    connection = None


@api_view(["GET"])
def query(request):

    data = {
        'pgsql': {
        }
    }
        
    cursor.execute("SELECT version();")
    data['pgsql']['version'] = cursor.fetchone()[0]
    cursor.execute("SELECT pg_database_size('dota2')/1024/1024")
    data['pgsql']['dota2_db_size'] = cursor.fetchone()[0]
    
    if(connection is not None):
        cursor.close()
        connection.close()

    return Response(data)

@api_view(["GET"])
def querry1(request):                           #endpoint cislo 1

    query =   '''SELECT patch_version, 
                patch_start_date::int, 
                patch_end_date::int,
                matches.id AS match_id, 
                ROUND(matches.duration::numeric/60,2) AS match_duration

                FROM (SELECT name AS patch_version,
                EXTRACT(EPOCH FROM release_date) AS patch_start_date,
				EXTRACT(EPOCH FROM LEAD(release_date, 1) OVER(ORDER BY release_date))
					  AS patch_end_date
                FROM patches ORDER BY name) AS tbl

                LEFT JOIN matches ON matches.start_time BETWEEN patch_start_date AND patch_end_date
                '''

    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    data = {
        "patches": []
    }

    temp = ""
    index = -1
    for row in result:
        
        if temp == row[0] and row[3] is not None:
            data["patches"][index]["matches"].append({"match_id":row[3], 
            "duration":row[4]})
        else:
            temp = row[0]
            index += 1

            data["patches"].append(
                {"patch_version": row[0], 
                "patch_start_date": row[1], 
                "patch_end_date": row[2], 
                "matches": [{
                    "match_id":row[3],
                    "duration":row[4]
                }] if row[3] is not None else []
                })
    return Response(data)

@api_view(["GET"])
def querry2(request,player_id):                         #endpoint cislo 2

    

    query =   f'''SELECT players.id, 
                coalesce(players.nick,'unknown') AS player_nick,
                matches.id AS match_id,
                heroes.localized_name AS hero_localized_name,
                ROUND(matches.duration/60.0,2) AS match_duration_minutes,
                coalesce(matches_players_details.xp_hero,0) + coalesce(matches_players_details.xp_creep,0) + coalesce(matches_players_details.xp_other,0) + coalesce(matches_players_details.xp_roshan,0) AS experiences_gained,
                matches_players_details.level AS level_gained,
                CASE 
                WHEN matches_players_details.player_slot BETWEEN 0 and 4 AND matches.radiant_win IS true THEN true
                WHEN matches_players_details.player_slot BETWEEN 128 and 132 AND matches.radiant_win IS false THEN true 
                ELSE false END
                as winner

                from players
                join matches_players_details on matches_players_details.player_id = players.id
                join heroes on heroes.id = matches_players_details.hero_id
                join matches on matches.id = matches_players_details.match_id
                WHERE players.id = {player_id}
                ORDER BY matches.id'''



    
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    data = {
        "id": result[0][0],
        "player_nick": result[0][1],
        "matches": []
    }

    for row in result:

        data["matches"].append(
            {  
                "match_id":row[2],
                "hero_localized_name":row[3],
                "match_duration_minutes":row[4],
                "experiences_gained":row[5],
                "level_gained":row[6],
                "winner":row[7],
            })

    return Response(data)

@api_view(["GET"])
def querry3(request,player_id):                                 #endpoint cislo 3

    

    query =   f'''SELECT DISTINCT
                players.id, 
                coalesce(players.nick,'unknown') AS player_nick,
                matches_players_details.match_id AS match_id,
                heroes.localized_name AS hero_localized_name,
                coalesce(game_objectives.subtype,'NO_ACTION') AS hero_action,
                GREATEST(COUNT(game_objectives.subtype), 1) AS count

                from players
                left join matches_players_details on matches_players_details.player_id = players.id
                left join heroes on heroes.id = matches_players_details.hero_id
                left join matches on matches.id = matches_players_details.match_id
                left join game_objectives ON game_objectives.match_player_detail_id_1 = matches_players_details.id
                WHERE players.id = {player_id}
                GROUP BY game_objectives.subtype, players.id, players.nick,matches_players_details.match_id, heroes.localized_name
                ORDER BY matches_players_details.match_id'''



    
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    data = {
        "id": result[0][0],
        "player_nick": result[0][1],
        "matches": []
    }

    temp = 0
    index = -1
    for row in result:

        if temp == row[2]:
            data["matches"][index]["actions"].append({"hero_action":row[4], 
            "count":row[5]})
        else:
            temp = row[2]
            index += 1

            data["matches"].append(
                {"match_id": row[2], 
                "hero_localized_name": row[3], 
                "actions": [{
                    "hero_action":row[4],
                    "count":row[5]
                }]})
    return Response(data)


@api_view(["GET"])
def querry4(request,player_id):                                         #endpoint cislo 4

    
    query =   f'''SELECT 
                    players.id, 
                    coalesce(players.nick, 'unknown') AS player_nick,
                    matches.id AS match_id,
                    heroes.localized_name AS hero_localized_name,
                    abilities.name AS ability_name,
                    count(ability_upgrades.id) AS count,
                    max(ability_upgrades.level) AS upgrade_level

                    FROM matches_players_details
                    JOIN players ON players.id = matches_players_details.player_id
                    JOIN heroes ON heroes.id = matches_players_details.hero_id
                    JOIN matches ON matches.id = matches_players_details.match_id
                    JOIN ability_upgrades ON ability_upgrades.match_player_detail_id = matches_players_details.id
                    JOIN abilities ON abilities.id = ability_upgrades.ability_id
                    WHERE players.id = {player_id}
                    GROUP BY abilities.name,matches.id,heroes.localized_name,players.id
                    ORDER BY matches.id
                    '''

    
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    data = {
        "id": result[0][0],
        "player_nick": result[0][1],
        "matches": []
    }

    temp = 0
    index = -1
    for row in result:
        

        if temp == row[2]:
            data["matches"][index]["abilities"].append({"ability_name":row[4], "count":row[5], 
            "upgrade_level":row[6]})
        else:
            temp = row[2]
            

            data["matches"].append(
                {"match_id": row[2], 
                "hero_localized_name": row[3], 
                "abilities": [{
                    "ability_name":row[4],
                    "count":row[5],
                    "upgrade_level":row[6]
                }]})
    
    return Response(data)

