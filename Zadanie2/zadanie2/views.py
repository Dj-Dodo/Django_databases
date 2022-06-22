from rest_framework.response import Response
from rest_framework.decorators import api_view
from django.shortcuts import render
import psycopg2 as psy
import environ

# Create your views here.
#request -> response


env = environ.Env()
environ.Env.read_env()


@api_view(["GET"])
def query(request):

    data = {
        'pgsql': {
        }
    }

    try:
        connection = psy.connect(
            user = env('DBUSER'),
            password = env('DBPASS'),
            host = "147.175.150.216",
            port = "5432",
            database = "dota2"
        )
        #cursor = pointer na databazu
        cursor = connection.cursor()
        
        cursor.execute("SELECT version();")
        data['pgsql']['version'] = cursor.fetchone()[0]
        cursor.execute("SELECT pg_database_size('dota2')/1024/1024")
        data['pgsql']['dota2_db_size'] = cursor.fetchone()[0]

    except(psy.Error):
        connection = None

    if(connection is not None):
        cursor.close()
        connection.close()

    return Response(data)