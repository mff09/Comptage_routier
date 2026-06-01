# Squelette GitHub — Pipeline ELT Comptage Routier

```
road-traffic-pipeline/
│
├── README.md                        # Description projet, archi diagram, screenshots dashboard
├── .gitignore                       # credentials.json, .env, __pycache__, *.egg-info
├── .env.example                     # Template variables d'environnement (jamais le vrai .env)
├── requirements.txt                 # Dépendances Python
│
├── ingestion/
│   ├── __init__.py
│   ├── fetch_traffic.py             # Appel API Paris Data, pagination, gestion erreurs
│   └── upload_gcs.py               # Upload fichier JSON/Parquet vers GCS bucket bronze/
│
├── transformation/
│   ├── __init__.py
│   ├── bronze_to_silver.py         # PySpark : lecture JSON GCS → nettoyage → Parquet silver/
│   └── silver_to_gold.py           # PySpark : agrégations → BigQuery table gold_traffic
│
├── dags/
│   ├── trafic_pipeline.py          # DAG Airflow principal (schedule quotidien)
│   └── utils/
│       └── gcp_helpers.py          # Fonctions utilitaires GCP partagées
│
├── config/
│   ├── settings.py                 # Variables de config (bucket names, dataset BQ, etc.)
│   └── schema.py                   # Schéma BigQuery de la table gold_traffic
│
├── docker/
│   └── docker-compose.yml          # Airflow standalone en local
│
├── tests/
│   ├── test_fetch.py               # Test unitaire ingestion (mock API)
│   └── test_transform.py           # Test transformation PySpark basique
│
├── notebooks/
│   └── exploration.ipynb           # Exploration données initiale (ne pas versionner les outputs)
│
└── .github/
    └── workflows/
        └── ci.yml                  # GitHub Actions : lint flake8 + tests (bonus)
```

## Contenu clé de chaque fichier

### .env.example
```
GCP_PROJECT_ID=your-project-id
GCS_BUCKET_BRONZE=road-traffic-bronze
GCS_BUCKET_SILVER=road-traffic-silver
BIGQUERY_DATASET=road_traffic
BIGQUERY_TABLE_GOLD=gold_traffic
GOOGLE_APPLICATION_CREDENTIALS=credentials.json
```

### .gitignore
```
credentials.json
.env
__pycache__/
*.pyc
*.egg-info/
.ipynb_checkpoints/
dist/
.DS_Store
```

### requirements.txt
```
google-cloud-storage==2.16.0
google-cloud-bigquery==3.20.0
pyspark==3.5.1
requests==2.31.0
python-dotenv==1.0.1
apache-airflow==2.9.1
pandas==2.2.2
pyarrow==16.0.0
```

## Commandes de démarrage

```bash
# Clone
git clone https://github.com/TON_USERNAME/road-traffic-pipeline
cd road-traffic-pipeline

# Environnement Python
python -m venv venv
source venv/bin/activate   # Windows : venv\Scripts\activate
pip install -r requirements.txt

# Config
cp .env.example .env
# → remplir le .env avec tes vraies valeurs GCP

# Lancer l'ingestion manuelle
python ingestion/fetch_traffic.py

# Lancer Airflow local
cd docker && docker-compose up
# → UI disponible sur http://localhost:8080
```

## Ordre de développement recommandé

1. `ingestion/fetch_traffic.py` — tester l'API en premier
2. `ingestion/upload_gcs.py` — valider le stockage GCS
3. `transformation/bronze_to_silver.py` — première transformation
4. `transformation/silver_to_gold.py` — agrégations BigQuery
5. `dags/trafic_pipeline.py` — connecter les étapes dans Airflow
6. Dashboard Looker Studio — depuis BigQuery directement
7. `tests/` + README + `.github/workflows/ci.yml` — finitions CV
