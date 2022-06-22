from django.urls import path
from . import views

#URL configuracia
urlpatterns = [
    path('v1/health', views.query),
    ]

