import requests
import json
import os

API_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_meteo(lat=48.8566, lon=2.3522, days=7):
    # Paris par défaut
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "forecast_days": days,
        "timezone": "Europe/Paris"
    }
    
    response = requests.get(API_URL, params=params, timeout=30)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Erreur API: {response.status_code}")

def save_locally(data, output_dir="data/meteo"):
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, "meteo.json")
    with open(filepath, "w") as f:
        json.dump(data, f)

if __name__ == "__main__":
    data = fetch_meteo()
    save_locally(data)
    print(f"OK — {len(data['hourly']['time'])} heures de météo sauvegardées")