from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
import json
import requests


#Latitude and Logitude for Munich
LATITUDE= '48.1351'
LONGITUDE= '11.5820'
POSTGRES_CONN_ID='postgres_etl'
API_CONN_ID='open_meteo_api'

default_args={
    'owner':'airflow'
}

#DAG
with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         start_date=datetime(2025, 8, 1),
         schedule='@daily',
         catchup=False) as dags:

    @task()
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using Airflow Connection."""

# HTTP Hook to get configuration details from Connection in Airflow

        http_hook = HttpHook(http_conn_id=API_CONN_ID, 
                             method='GET')

#Build API endpoint
#Base URL - https://api.open-meteo.com/v1/forecast?latitude=48.1351&longitude=11.5820&current_weather=true
        end_point=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

#Make request to API through HTTP Hook
        response = http_hook.run(end_point)

#if issue in fetching data, following message can be seen in logs in Airflow UI
        if response.status_code==200:
            return response.json()

        else:
            raise Exception (f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather= weather_data['current_weather']
        transform_data= {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        } 
        return transform_data 

    @task()
    def load_weather_data(transform_data):
        """Load transformed data into PostgreSQL."""
        pg_hook=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn =pg_hook.get_conn()
        cursor = conn.cursor()

    #Created table outline in Postgres database 
        cursor.execute ("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT, 
            weathercode INT,
            city VARCHAR(50),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
    # Insert transformed data into table
        cursor.execute ("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode, city)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            transform_data['latitude'],
            transform_data['longitude'],
            transform_data['temperature'],
            transform_data['windspeed'],
            transform_data['winddirection'],
            transform_data['weathercode'],
            'Munich'
        ))

        conn.commit()
        cursor.close()
        conn.close()

    # Create worflow with dependencies
    weather_data= extract_weather_data()
    transform_data=transform_weather_data(weather_data)
    load_weather_data(transform_data)
