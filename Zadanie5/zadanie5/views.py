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

@api_view(["GET"])
def querry31(request,match_id):

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

    query = f"""SELECT *
                FROM (
                    SELECT table1.match_id,
                        table1.id,
                        table1.localized_name,
                        table1.itemid,
                        table1.name,
                        table1.count, 
                        ROW_NUMBER() OVER(PARTITION BY id ORDER BY count DESC) AS rownum

                    FROM(
                        SELECT match_id,
                                heroes.id,
                                heroes.localized_name,
                                items.id AS itemid,
                                items.name,
                                COUNT(items.name) AS count,
                                CASE
                                WHEN (matches_players_details.player_slot >= 0 AND matches_players_details.player_slot <= 4) = matches.radiant_win THEN true
                                ELSE false
                                END AS winner

                        FROM matches_players_details
                        JOIN matches ON matches.id = matches_players_details.match_id
                        JOIN heroes ON matches_players_details.hero_id = heroes.id
                        JOIN purchase_logs ON purchase_logs.match_player_detail_id = matches_players_details.id
                        JOIN items ON purchase_logs.item_id = items.id

                        WHERE match_id = {match_id}
                        GROUP BY items.name, match_id, items.id,heroes.id,heroes.name, matches_players_details.player_slot, matches.radiant_win
                        ORDER BY 
                            heroes.id,
                            count DESC,
                            items.name

                        ) table1 
                        WHERE winner = 'true'
                    ) table2
                    WHERE rownum <= 5"""

    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    data = {
        "id": result[0][0],
        "heroes": []
    }
    temp = 0
    index = -1
    for row in result:
        if temp == row[1]:
            data["heroes"][index]["top_purchases"].append({"id":row[3], "name":row[4], "count":row[5]})
        else:
            temp = row[1]

            data["heroes"].append(
                {"id":row[1],
                "name":row[2],
                "top_purchases": [{
                    "id":row[3],
                    "name":row[4],
                    "count":row[5]
                    }]})
    return Response(data)

@api_view(["GET"])
def querry32(request,ability_id):

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
    
    query = f"""SELECT *
                FROM (
                    SELECT *, 
                        COUNT(bucket), 
                        ROW_NUMBER() OVER(PARTITION BY hero_name, winner ORDER BY COUNT(ab_name) DESC) AS rownum
                        FROM (
                            SELECT abilities.id AS ab_id, 
                                    abilities.name AS ab_name,
                                    heroes.id AS hero_id,
                                    heroes.localized_name AS hero_name,
                                    CASE 
                                        WHEN matches_players_details.player_slot BETWEEN 0 and 4 AND matches.radiant_win IS true THEN true
                                        WHEN matches_players_details.player_slot BETWEEN 128 and 132 AND matches.radiant_win IS false THEN true 
                                        ELSE false 
                                    END AS winner,
                                    CASE 
                                        WHEN 100*ability_upgrades.time/matches.duration >= '0' AND 100*ability_upgrades.time/matches.duration < '10' THEN '0-9'
                                        WHEN 100*ability_upgrades.time/matches.duration >= '10' AND 100*ability_upgrades.time/matches.duration < '20' THEN '10-19'
                                        WHEN 100*ability_upgrades.time/matches.duration >= '20' AND 100*ability_upgrades.time/matches.duration < '30' THEN '20-29'
                                        WHEN 100*ability_upgrades.time/matches.duration >= '30' AND 100*ability_upgrades.time/matches.duration < '40' THEN '30-39'
                                        WHEN 100*ability_upgrades.time/matches.duration >= '40' AND 100*ability_upgrades.time/matches.duration < '50' THEN '40-49'
                                        WHEN 100*ability_upgrades.time/matches.duration >= '50' AND 100*ability_upgrades.time/matches.duration < '60' THEN '50-59'
                                        WHEN 100*ability_upgrades.time/matches.duration >= '60' AND 100*ability_upgrades.time/matches.duration < '70' THEN '60-69' 
                                        WHEN 100*ability_upgrades.time/matches.duration >= '70' AND 100*ability_upgrades.time/matches.duration < '80' THEN '70-79' 
                                        WHEN 100*ability_upgrades.time/matches.duration >= '80' AND 100*ability_upgrades.time/matches.duration < '90' THEN '80-89' 
                                        WHEN 100*ability_upgrades.time/matches.duration >= '90' AND 100*ability_upgrades.time/matches.duration < '100' THEN '90-99' 
                                        WHEN 100*ability_upgrades.time/matches.duration >= '100' AND 100*ability_upgrades.time/matches.duration < '110' THEN '100-109'
                                    END AS bucket
                            FROM matches_players_details
                            LEFT JOIN ability_upgrades ON ability_upgrades.match_player_detail_id = matches_players_details.id
                            LEFT JOIN abilities ON abilities.id = ability_upgrades.ability_id
                            LEFT JOIN heroes ON heroes.id = matches_players_details.hero_id
                            LEFT JOIN matches ON matches.id = matches_players_details.match_id
                            WHERE abilities.id = {ability_id}
                        ) AS table2
                        GROUP BY ab_id, ab_name, hero_id, hero_name, bucket, winner
                    ) AS table1
                    WHERE rownum <= 1 """

    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    data = {
        "heroes":[],
        "id":result[0][0],
        "name":result[0][1]
    }
    temp = 0
    for row in result:
        if temp != row[3] and row[4] == True:               #poradie: winners , loosers
            temp = row[3]
            data["heroes"].append(
                {
                    "id": row[2],
                    "name": row[3],
                    "usage_winners":{
                        "bucket": row[5],
                        "count": row[6]
                    },
                }
            )
        elif temp == row[3] and row[4] == False:
            data["heroes"][-1]["usage_loosers"] = {"bucket": row[5],
                "count": row[6]}
        elif temp != row[3] and row[4] == False:            #poradie: loosers, winners
            temp = row[3]
            data["heroes"].append(
                {
                    "id": row[2],
                    "name": row[3],
                    "usage_loosers":{
                        "bucket": row[5],
                        "count": row[6]
                    },
                }
            )
        elif temp == row[3] and row[4] == True:
            data["heroes"][-1]["usage_winners"] = {"bucket": row[5],
                "count": row[6]}
        
    return Response(data)

@api_view(["GET"])
def querry33(request):

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
    
    query = """SELECT 
                    table3.hero_id AS id, 
                    table3.localized_name AS name, 
                    table3.tower_kills AS tower_kills
                FROM (
                    SELECT 
                        table2.hero_id, 
                        table2.localized_name, 
                        COUNT(table2.tower_kills) AS tower_kills,
                        ROW_NUMBER() OVER(PARTITION BY table2.localized_name ORDER BY COUNT(TOWER_KILLS) DESC) AS row_num
                    FROM(
                        SELECT 
                            table1.hero_id, 
                            table1.localized_name, 
                            COUNT(*) FILTER(WHERE table1.lag) OVER(ORDER BY table1.id, table1.go_time) tower_kills
                        FROM(
                            SELECT
                                matches_players_details.match_id as id, 
                                heroes.id AS hero_id, 
                                heroes.localized_name AS localized_name, 
                                heroes.localized_name IS DISTINCT FROM LAG(heroes.localized_name, 1) OVER(PARTITION BY matches_players_details.match_id ORDER BY matches.duration) AS lag, 
								game_objectives.time AS go_time
                            FROM game_objectives
                                JOIN matches_players_details ON game_objectives.match_player_detail_id_1 = matches_players_details.id
                                JOIN heroes ON matches_players_details.hero_id = heroes.id
                                JOIN matches ON matches_players_details.match_id = matches.id
                            WHERE game_objectives.subtype = 'CHAT_MESSAGE_TOWER_KILL'
                            ORDER BY matches_players_details.match_id
                        ) table1
                    ) table2
                    GROUP BY table2.tower_kills, table2.localized_name, table2.hero_id
                ) table3
                WHERE row_num = 1
                ORDER BY table3.tower_kills DESC, table3.localized_name"""
    
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    data = {
        "heroes":[]
    }

    for row in result:
        data["heroes"].append({
            "id":row[0],
            "name":row[1],
            "tower_kills":row[2]
        })   

    return Response(data)