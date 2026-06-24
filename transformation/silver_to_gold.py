# transformation/silver_to_gold.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    return SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

def load_silver(spark, silver_dir="data/silver"):
    return spark.read.parquet(silver_dir)

def gold_debit_horaire(df):
    # Débit moyen par heure et par tronçon
    return df.groupBy("libelle", "heure") \
             .agg(
                 F.avg("q").alias("debit_moyen"),
                 F.avg("k").alias("occupation_moyenne"),
                 F.count("*").alias("nb_mesures")
             ) \
             .orderBy("libelle", "heure")

def gold_top_troncons(df):
    # Top tronçons par débit moyen
    return df.groupBy("libelle") \
             .agg(F.avg("q").alias("debit_moyen")) \
             .orderBy(F.desc("debit_moyen"))

def save_gold(df, name, output_dir="data/gold"):
    df.write.mode("overwrite").parquet(f"{output_dir}/{name}")

def load_vacances(spark, vacances_path="data/vacances/vacances.json"):
    return spark.read.json(vacances_path)

def gold_trafic_vacances(df_trafic, df_vacances):
    # Extraire juste les dates de début et fin de vacances
    df_vacances = df_vacances.select(
        F.to_timestamp("start_date").alias("debut_vacances"),
        F.to_timestamp("end_date").alias("fin_vacances"),
        "description"
    )
    
    # Croiser : est-ce que la mesure trafic tombe dans une période de vacances ?
    df = df_trafic.crossJoin(df_vacances) \
        .withColumn("en_vacances", 
            (F.col("t_1h") >= F.col("debut_vacances")) & 
            (F.col("t_1h") <= F.col("fin_vacances"))
        ) \
        .groupBy("en_vacances") \
        .agg(F.avg("q").alias("debit_moyen"))
    
    return df

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