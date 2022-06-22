from django.urls import path
from . import views

#URL configuracia
urlpatterns = [
    path('v1/health', views.query),

    path('v2/patches/', views.querry1),
    path('v2/players/<int:player_id>/game_exp/', views.querry2),
    path('v2/players/<int:player_id>/game_objectives/',views.querry3),
    path('v2/players/<int:player_id>/abilities/', views.querry4), 

    path('v3/matches/<int:match_id>/top_purchases/', views.querry31), 
    path('v3/abilities/<int:ability_id>/usage/', views.querry32), 
    path('v3/statistics/tower_kills/', views.querry33)
    ]

