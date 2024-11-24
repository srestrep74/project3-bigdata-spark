from django.urls import path
from . import views

urlpatterns = [
    path('', views.covid_dashboard, name='covid_dashboard'),
]