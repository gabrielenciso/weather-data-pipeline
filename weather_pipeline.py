import os
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from prefect import flow, task

load_dotenv()
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")

@task(name="Fetch Weather Data", retries=3, retry_delay_seconds=10)
def fetch_weather_data(city: str) -> dict:
  """Fetch weather data for a specific city"""
  url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
  response = requests.get(url)
  response.raise_for_status() # Raise error for bad status codes
  return response.json()

@task(name="Process Data")
def process_weather_data(raw_data: dict) -> pd.DataFrame:
  """Process raw API data into a structured DataFrame."""
  # Extract relevant fields
  processed_data = {
    "city": raw_data["name"],
    "timestamp": pd.to_datetime(raw_data["dt"], unit="s"),
    "temperature": raw_data["main"]["temp"],
    "humidity": raw_data["main"]["humidity"],
    "rain": raw_data.get("rain", {}).get("1h", 0),
    "wind_speed": raw_data["wind"]["speed"]
  }

  df = pd.DataFrame([processed_data])
  return df

@task(name="Save to PostgreSQL")
def save_to_postgres(df: pd.DataFrame, table_name: str = "weather_metrics") -> None:
  """Save DataFrame to PostgreSQL database."""
  try:
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    # Create conenction string
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(conn_str)

    # Save to database
    df.to_sql(
      name=table_name,
      con=engine,
      if_exists="append",
      index=False
    )
    print(f"Data saved to {table_name}!")
  except SQLAlchemyError as e:
    print(f"Database error: {e}")

@task(name="Generate CSV Report")
def generate_csv_report(df: pd.DataFrame, filename: str = "weather_report.csv") -> None:
  """Save the processed DataFram to a CSV file"""
  df.to_csv(filename, index=False)
  print(f"CSV report saved as {filename}")

@flow(name="Weather Data Pipeline")
def weather_pipeline(city: str = "San Francisco"):
    raw_data = fetch_weather_data(city)
    df = process_weather_data(raw_data)
    save_to_postgres(df)
    generate_csv_report(df)
    return df


def test_postgres_connection():
  try:
    conn_str = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    engine = create_engine(conn_str)
    conn = engine.connect()
    print("Connected to PostgreSQL successfully!")
    conn.close()
  except Exception as e:
    print (f"Connection failed: {e}")

if __name__ == "__main__":
  raw_data = fetch_weather_data("San Francisco")
  df = process_weather_data(raw_data)
  save_to_postgres(df)
  print("Processed Data:")
  print(df)
