# Comptage Routier Paris — ELT Pipeline

"Faut jamais aller à Paris en voiture" — on l'entend tout le temps.
Même en tant que piétonne, je le vois sur certaines avenues : les bouchons parisiens ne sont pas un mythe.

Alors j'ai voulu investiguer : quels facteurs favorisent autant d'embouteillages ?
Mon hypothèse de départ : la météo et les vacances scolaires. Dans les transports en commun, les ralentissements et variations d'affluence sont souvent liés à ces deux facteurs — on va voir ce que ça donne sur la route.

Ce projet est un pipeline ELT end-to-end qui croise les données de comptage routier parisien avec la météo et le calendrier scolaire.

## Stack

| Couche | Outil |
|--------|-------|
| Ingestion | Python + requests |
| Stockage | Google Cloud Storage |
| Transformation | PySpark |
| Serving | BigQuery |
| Dashboard | Looker Studio |
| Orchestration | Apache Airflow (Docker) |

## Structure

```
├── ingestion/
│   ├── fetch_trafic.py       # API Paris Data (comptage routier)
│   ├── fetch_vacances.py     # API Éducation Nationale (vacances Zone C)
│   └── fetch_meteo.py        # API Open-Meteo (météo Paris)
├── transformation/
│   ├── bronze_to_silver.py   # Nettoyage et typage PySpark
│   └── silver_to_gold.py     # Agrégations + enrichissements
├── dags/
│   └── trafic_pipeline.py    # DAG Airflow
└── data/                     # Ignoré par git
```

## Installation

```bash
git clone https://github.com/mff09/Comptage_routier.git
cd Comptage_routier
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Lancer le pipeline

```bash
python ingestion/fetch_trafic.py
python ingestion/fetch_vacances.py
python ingestion/fetch_meteo.py

python transformation/bronze_to_silver.py
python transformation/silver_to_gold.py
```

## Sources

- [Comptage routier Paris](https://opendata.paris.fr/explore/dataset/comptages-routiers-permanents/api/)
- [Calendrier scolaire Zone C](https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/fr-en-calendrier-scolaire/records)
- [Open-Meteo](https://api.open-meteo.com)