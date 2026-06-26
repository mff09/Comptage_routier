# ingestion/fetch_vacances.py

import requests
import json
import os
from datetime import datetime

API_URL = "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/records"

def fetch_vacances(zone="Zone C", limit=100):
    """
    Récupère les données de vacances pour la zone C (Paris)
    """
    params = {
        "limit": limit,
        "refine": f'zones:"{zone}"'
    }
    response = requests.get(API_URL, params=params, timeout=30)
    
    if response.status_code == 200:
        return response.json()["results"]
    else:
        raise Exception(f"Erreur API: {response.status_code}")

def save_locally(data, output_dir="data/vacances"):
    """
    Sauvegarde les données de vacances localement
    """
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, "vacances.json")
    with open(filepath, "w") as f:
        json.dump(data, f)

if __name__ == "__main__":
    data = fetch_vacances()
    save_locally(data)
    print(f"OK — {len(data)} périodes de vacances sauvegardées")