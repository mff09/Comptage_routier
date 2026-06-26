PROJECT_ID = "paris-trafic-pipeline"
BQ_DATASET = "trafic_routier"
BQ_TABLES = {
    "debit_horaire": "gold_debit_horaire",
    "top_troncons": "gold_top_troncons",
    "trafic_vacances": "gold_trafic_vacances",
    "trafic_meteo": "gold_trafic_meteo"
}
GCS_BUCKET = "paris-trafic-bronze"