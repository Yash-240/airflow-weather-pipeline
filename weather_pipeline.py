from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import sqlite3
import os

# Configs
API_KEY = '*********'
CITY = 'Düsseldorf'
DB_PATH = '/opt/airflow/dags/weather_data.db'

def fetch_weather_and_store(**kwargs):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()

    city = data["name"]
    dt = datetime.utcfromtimestamp(data["dt"]).strftime('%Y-%m-%d %H:%M:%S')
    temperature = data["main"]["temp"]
    humidity = data["main"]["humidity"]
    description = data["weather"][0]["description"]

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city TEXT,
            datetime TEXT,
            temperature REAL,
            humidity REAL,
            weather_description TEXT
        )
    """)

    # Insert data
    cursor.execute("""
        INSERT INTO weather_data (city, datetime, temperature, humidity, weather_description)
        VALUES (?, ?, ?, ?, ?)
    """, (city, dt, temperature, humidity, description))

    conn.commit()
    conn.close()
    print("✅ Weather data inserted into SQLite!")

def generate_daily_summary(**kwargs):
    today_date = datetime.utcnow().strftime('%Y-%m-%d')

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 
            AVG(temperature),
            AVG(humidity)
        FROM weather_data
        WHERE DATE(datetime) = ?
    """, (today_date,))

    row = cursor.fetchone()
    conn.close()

    summary = f"📊 Weather Summary for {today_date}:\n"
    summary += f"Average Temperature: {row[0]:.2f}°C\n"
    summary += f"Average Humidity: {row[1]:.2f}%\n"

    print(summary)
    return summary

default_args = {
    'owner': '*********',                           #write your name
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_weather_to_sqlite',
    default_args=default_args,
    description='Fetch daily weather data and store into SQLite',
    start_date=datetime(2024, 4, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fetch_weather_task = PythonOperator(
        task_id='fetch_weather_and_store',
        python_callable=fetch_weather_and_store,
    )

    generate_summary_task = PythonOperator(
        task_id='generate_daily_summary',
        python_callable=generate_daily_summary,
    )

    fetch_weather_task >> generate_summary_task