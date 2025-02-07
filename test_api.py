import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
CITY = "San Francisco"

url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
response = requests.get(url)

try:
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for HTTP issues (4xx/5xx)
    print("Success! Response:", response.json())
except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e}")
except Exception as e:
    print(f"Error: {e}")
