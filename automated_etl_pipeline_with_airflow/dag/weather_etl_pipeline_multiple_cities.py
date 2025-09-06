from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
import json
import requests
import pandas as pd


POSTGRES_CONN_ID = 'postgres_etl'
API_CONN_ID = 'open_meteo_api'

default_args = {'owner': 'airflow'}

with DAG(
    dag_id='weather_etl_pipeline_multiple_cities',
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule='@daily',
    catchup=False
) as dag:

    @task()
    def get_city_location():
        """Fetch list of cities dynamically from Postgres into a list of dicts."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT city, latitude, longitude FROM list_cities;")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return [{"city": r[0], "latitude": r[1], "longitude": r[2]} for r in rows]

    @task()
    def extract_weather_data(location:dict):
        city = location["city"]
        latitude = location["latitude"]
        longitude = location["longitude"]

        """Extract weather data from Open-Meteo API for a single city."""

        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        end_point = f'/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true'
        response = http_hook.run(end_point)

        if response.status_code == 200:
            data = response.json()
            current = data["current_weather"]
            return {
                "city": city,
                "latitude": latitude,
                "longitude": longitude,
                "temperature": current["temperature"],
                "windspeed": current["windspeed"],
                "winddirection": current["winddirection"],
                "weathercode": current["weathercode"]
            }
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_records: list[dict]):
        """Transform list of weather dicts into a pandas DataFrame."""
        df = pd.DataFrame(weather_records)
        # Example transformation: enforce data types
        df = df.astype({
            "latitude": "float",
            "longitude": "float",
            "temperature": "float",
            "windspeed": "float",
            "winddirection": "float",
            "weathercode": "int"
        })
        return df.to_dict(orient="records")  # return list of dicts for XCom compatibility

    @task()
    def load_weather_data(weather_dicts: list[dict]):
        """Load pandas DataFrame into PostgreSQL."""
        df = pd.DataFrame(weather_dicts)

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data_cities (
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

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO weather_data_cities 
                (latitude, longitude, temperature, windspeed, winddirection, weathercode, city)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row["latitude"], 
                row["longitude"], 
                row["temperature"],
                row["windspeed"], 
                row["winddirection"], 
                row["weathercode"],
                row["city"]
            ))

        conn.commit()
        cursor.close()
        conn.close()

    # DAG workflow
    city_location = get_city_location()
    weather_data = extract_weather_data.expand(location=city_location)
    df_transform_weather_data = transform_weather_data(weather_records=weather_data) 
    load_weather_data(df_transform_weather_data)


