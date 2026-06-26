import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    """
    Lance le moteur spark et crée une session Spark
    """
    return SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

def load_silver(spark, silver_dir="data/silver"):
    """
    Récupère les données silver depuis le répertoire spécifié
    """
    return spark.read.parquet(silver_dir)

def gold_debit_horaire(df):
    """
    Calcule le debit moyen par heure et par tronçon 
    """
    return df.groupBy("libelle", "heure") \
             .agg(
                 F.avg("q").alias("debit_moyen"),
                 F.avg("k").alias("occupation_moyenne"),
                 F.count("*").alias("nb_mesures")
             ) \
             .orderBy("libelle", "heure")

def gold_top_troncons(df):
    """
    Classe les tronçons par debit moyen décroissant
    """
    return df.groupBy("libelle") \
             .agg(F.avg("q").alias("debit_moyen")) \
             .orderBy(F.desc("debit_moyen"))

def save_gold(df, name, output_dir="data/gold"):
    """
    Sauvegarde les données gold dans le repertoire spécifié 
    """
    df.write.mode("overwrite").parquet(f"{output_dir}/{name}")

def load_vacances(spark, vacances_path="data/vacances/vacances.json"):
    """
    Charge les données de vacances récupérées par le script ingestion/fetch_vacances.py
    """
    return spark.read.json(vacances_path)

def gold_trafic_vacances(df_trafic, df_vacances):
    """
    Données de trafic x données de vacances 
    - Extrait les periodes de vacance
    - Croise les données de trafic avec les périodes de vacances
    - Calcule le débit moyen par période de vacances
    """
    df_vacances = df_vacances.select(
        F.to_timestamp("start_date").alias("debut_vacances"),
        F.to_timestamp("end_date").alias("fin_vacances"),
        "description"
    )
    
    # Croiser : est-ce que la mesure trafic tombe dans sur une période de vacances ?
    df = df_trafic.crossJoin(df_vacances) \
        .withColumn("en_vacances", 
            (F.col("t_1h") >= F.col("debut_vacances")) & 
            (F.col("t_1h") <= F.col("fin_vacances"))
        ) \
        .groupBy("en_vacances") \
        .agg(F.avg("q").alias("debit_moyen"))
    
    return df


def load_meteo(spark, meteo_path="data/meteo/meteo.json"):
    """
    Charge les données météo récupérées par le script ingestion/fetch_meteo.py
    """
    with open(meteo_path) as f:
        raw = json.load(f)
    
    hourly = raw["hourly"]
    rows = list(zip(hourly["time"], hourly["temperature_2m"], hourly["precipitation"], hourly["windspeed_10m"]))
    
    return spark.createDataFrame(rows, ["time", "temperature", "precipitation", "vent"])

def gold_trafic_meteo(df_trafic, df_meteo):
    """
    Données de trafic x données météo
    - Croise les données de trafic avec les données météo
    - Calcule le débit moyen par température et précipitation
    """
    df_meteo = df_meteo.withColumn("time", F.to_timestamp("time"))
    
    return df_trafic.join(df_meteo, 
        F.date_trunc("hour", df_trafic["t_1h"]) == F.date_trunc("hour", df_meteo["time"]),
        "left"
    ).groupBy("temperature", "precipitation") \
     .agg(F.avg("q").alias("debit_moyen")) \
     .orderBy("precipitation")


if __name__ == "__main__":
    spark = create_spark_session()
    df = load_silver(spark)

    debit_horaire = gold_debit_horaire(df)
    top_troncons = gold_top_troncons(df)

    save_gold(debit_horaire, "debit_horaire")
    save_gold(top_troncons, "top_troncons")

    print(f"OK — debit_horaire: {debit_horaire.count()} lignes")
    print(f"OK — top_troncons: {top_troncons.count()} lignes")
    df_vacances = load_vacances(spark)
    trafic_vacances = gold_trafic_vacances(df, df_vacances)
    save_gold(trafic_vacances, "trafic_vacances")
    print(f"OK — trafic_vacances: {trafic_vacances.count()} lignes")

    df_meteo = load_meteo(spark)
    trafic_meteo = gold_trafic_meteo(df, df_meteo)
    save_gold(trafic_meteo, "trafic_meteo")
    print(f"OK — trafic_meteo: {trafic_meteo.count()} lignes")