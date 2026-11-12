import requests, pandas as pd, os

CITIES = {
    "Delhi": (28.61, 77.21), "Mumbai": (19.08, 72.88),
    "Chennai": (13.08, 80.27), "Bengaluru": (12.97, 77.59),
    "Hyderabad": (17.38, 78.48),
}
URL = "https://archive-api.open-meteo.com/v1/archive"
os.makedirs("data/raw", exist_ok=True)

for city, (lat, lon) in CITIES.items():
    r = requests.get(URL, params={
        "latitude": lat, "longitude": lon,
        "start_date": "2015-01-01", "end_date": "2024-12-31",
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "timezone": "Asia/Kolkata",
    }, timeout=120)
    r.raise_for_status()
    df = pd.DataFrame(r.json()["hourly"]).rename(columns={
        "temperature_2m": "temperature", "relative_humidity_2m": "humidity",
        "precipitation": "rainfall", "wind_speed_10m": "wind_speed"})
    df.insert(0, "city", city)
    df.to_csv(f"data/raw/{city}.csv", index=False)
    print(city, len(df), "rows")