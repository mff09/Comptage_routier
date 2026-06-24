from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def create_spark_session():
    return SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()

def load_bronze(spark, bronze_dir="data/raw"):
    return spark.read.json(bronze_dir)

def transform(df):
    df = df.select("iu_ac", "libelle", "t_1h", "q", "k", "etat_trafic", "geo_point_2d")
    df = df.withColumn("t_1h", F.to_timestamp("t_1h"))
    df = df.withColumn("heure", F.hour("t_1h"))
    df = df.withColumn("jour_semaine", F.dayofweek("t_1h"))
    df = df.filter(F.col("q").isNotNull())
    return df

def save_silver(df, output_dir="data/silver"):
    df.write.mode("overwrite").parquet(output_dir)




if __name__ == "__main__":
    spark = create_spark_session()
    df = load_bronze(spark)
    df = transform(df)
    save_silver(df)
    print(f"OK — {df.count()} enregistrements sauvegardés")