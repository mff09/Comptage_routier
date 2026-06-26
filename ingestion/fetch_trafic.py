import requests
import json
import os
from datetime import datetime

API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/comptages-routiers-permanents/records"


def fetch_traffic_data(limit=100, date=None):
    """
    Récupère les données de trafic pour une date donnée
    """
    if date is None:
        date = datetime.now().strftime("%Y/%m/%d")
    
    params = {
        "limit": limit,
        "timezone": "Europe/Paris",
        "refine": f't_1h:"{date}"'
    }
    
    response = requests.get(API_URL, params=params)
    
    if response.status_code == 200:
        return response.json()["results"]
    else:
        raise Exception(f"Erreur API: {response.status_code} - {response.text}")
    
    return response.json()["results"]


def save_locally(data, output_dir="data/raw"):
    """
    Sauvegarde les données de trafic localement
    """
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"traffic_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, "w") as f:
        json.dump(data, f)


if __name__ == "__main__":
    data = fetch_traffic_data()
    save_locally(data)
    print(f"OK — {len(data)} enregistrements sauvegardés")