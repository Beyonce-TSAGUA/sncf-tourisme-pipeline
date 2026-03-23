"""
=====================================================================
Spark Structured Streaming — Pipeline SNCF Tourisme en Train
=====================================================================
Rôle :
    Ce job Spark lit en continu les 4 topics Kafka produits par
    le producer SNCF et écrit les données transformées en format
    Parquet dans MinIO (couche Silver).

Flux :
    Kafka (4 topics) → Spark Structured Streaming → MinIO (Parquet)

Topics consommés :
    - sncf-gares          → silver/gares/
    - sncf-frequentation  → silver/frequentation/
    - sncf-tgvmax         → silver/tgvmax/
    - sncf-regularite     → silver/regularite/

Transformations appliquées :
    - Parsing JSON des messages Kafka
    - Nettoyage et typage des colonnes
    - Enrichissement (coordonnées GPS, calcul CO2, geo_cell)
    - Écriture Parquet compressé Snappy dans MinIO

Utilisation :
    python spark_jobs/streaming_job.py

Prérequis :
    - Kafka doit tourner (docker compose up -d)
    - MinIO doit tourner (docker compose up -d)
    - Producer doit avoir publié des données
    - Variables d'environnement dans .env
=====================================================================
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType
)

# ─────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────
load_dotenv()

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")

# Chemin de base dans MinIO pour la couche Silver
SILVER_BASE = "s3a://silver"

# Checkpoint local Windows — évite le problème NativeIO$Windows
CHECKPOINT_BASE = "file:///C:/tmp/spark_checkpoints"

# Topics Kafka à consommer
TOPICS = {
    "gares":         "sncf-gares",
    "frequentation": "sncf-frequentation",
    "tgvmax":        "sncf-tgvmax",
    "regularite":    "sncf-regularite",
}


# ─────────────────────────────────────────────────────────────────
# SCHÉMAS DES DONNÉES
# ─────────────────────────────────────────────────────────────────

SCHEMA_GARES = StructType([
    StructField("nom",          StringType(), True),
    StructField("libellecourt", StringType(), True),
    StructField("codes_uic",    StringType(), True),
    StructField("segment_drg",  StringType(), True),
    StructField("codeinsee",    StringType(), True),
    StructField("position_geographique", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
    ]), True),
    StructField("ingested_at",  StringType(), True),
])

SCHEMA_FREQUENTATION = StructType([
    StructField("nom_gare",              StringType(),  True),
    StructField("code_uic_complet",      StringType(),  True),
    StructField("code_postal",           StringType(),  True),
    StructField("segmentation_drg",      StringType(),  True),
    StructField("total_voyageurs_2024",  DoubleType(),  True),
    StructField("total_voyageurs_2023",  DoubleType(),  True),
    StructField("total_voyageurs_2022",  DoubleType(),  True),
    StructField("total_voyageurs_2019",  DoubleType(),  True),
    StructField("ingested_at",           StringType(),  True),
])

SCHEMA_TGVMAX = StructType([
    StructField("date",             StringType(), True),
    StructField("train_no",         StringType(), True),
    StructField("axe",              StringType(), True),
    StructField("origine",          StringType(), True),
    StructField("destination",      StringType(), True),
    StructField("origine_iata",     StringType(), True),
    StructField("destination_iata", StringType(), True),
    StructField("heure_depart",     StringType(), True),
    StructField("heure_arrivee",    StringType(), True),
    StructField("od_happy_card",    StringType(), True),
    StructField("ingested_at",      StringType(), True),
])

SCHEMA_REGULARITE = StructType([
    StructField("date",                       StringType(),  True),
    StructField("gare_depart",                StringType(),  True),
    StructField("gare_arrivee",               StringType(),  True),
    StructField("nb_train_prevu",             IntegerType(), True),
    StructField("nb_annulation",              IntegerType(), True),
    StructField("nb_train_retard_arrivee",    IntegerType(), True),
    StructField("retard_moyen_arrivee",       DoubleType(),  True),
    StructField("nb_train_retard_sup_15",     IntegerType(), True),
    StructField("nb_train_retard_sup_30",     IntegerType(), True),
    StructField("prct_cause_externe",         DoubleType(),  True),
    StructField("prct_cause_infra",           DoubleType(),  True),
    StructField("prct_cause_materiel_roulant",DoubleType(),  True),
    StructField("ingested_at",                StringType(),  True),
])


# ─────────────────────────────────────────────────────────────────
# CRÉATION DE LA SESSION SPARK
# ─────────────────────────────────────────────────────────────────

def creer_spark_session() -> SparkSession:
    """
    Crée et configure la session Spark avec :
    - Connecteur Kafka (lecture streaming)
    - Connecteur S3A (écriture MinIO)
    - Hadoop AWS (authentification MinIO)
    - Blocs S3A en mémoire (évite NativeIO$Windows)
    - Checkpoint local (évite NativeIO$Windows)
    """
    # Fix Windows : HADOOP_HOME requis par Spark
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

    print("Démarrage de la session Spark...")

    spark = (
        SparkSession.builder
        .appName("SNCF-Tourisme-Streaming")
        # ── Packages Maven nécessaires ──────────────────────────
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        # ── Configuration MinIO (S3-compatible) ─────────────────
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # ── Fix NativeIO Windows : blocs S3A en mémoire ─────────
        # Sans ça, S3A écrit des fichiers temporaires sur le disque
        # local via NativeIO qui plante sur Windows
        .config("spark.hadoop.fs.s3a.fast.upload",        "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.multipart.size",     "67108864")
        # ── Fix filesystem local Windows ─────────────────────────
        .config("spark.hadoop.fs.file.impl",
                "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
        # ── Optimisations ────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Session Spark démarrée.")
    return spark


# ─────────────────────────────────────────────────────────────────
# FONCTION GÉNÉRIQUE DE LECTURE KAFKA
# ─────────────────────────────────────────────────────────────────

def lire_topic_kafka(spark: SparkSession, topic: str):
    """
    Lit un topic Kafka en mode streaming.
    Retourne un DataFrame streaming avec colonne 'json_str'.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value").cast("string").alias("json_str"))
    )


# ─────────────────────────────────────────────────────────────────
# TRANSFORMATIONS PAR DATASET
# ─────────────────────────────────────────────────────────────────

def transformer_gares(df_raw):
    """
    Parse les gares, extrait lat/lon, calcule geo_cell.
    geo_cell = clé spatiale ~10km pour jointures rapides.
    """
    return (
        df_raw
        .select(F.from_json("json_str", SCHEMA_GARES).alias("d"))
        .select(
            F.col("d.nom").alias("nom_gare"),
            F.col("d.libellecourt").alias("trigramme"),
            F.col("d.codes_uic").alias("codes_uic"),
            F.col("d.segment_drg").alias("segment_drg"),
            F.col("d.codeinsee").alias("code_insee"),
            F.col("d.position_geographique.lat").alias("latitude"),
            F.col("d.position_geographique.lon").alias("longitude"),
            F.col("d.ingested_at").alias("ingested_at"),
        )
        .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .withColumn("lat_grid", F.round("latitude",  2))
        .withColumn("lon_grid", F.round("longitude", 2))
        .withColumn("geo_cell",
            F.concat(F.col("lat_grid").cast("string"),
                     F.lit("_"),
                     F.col("lon_grid").cast("string"))
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def transformer_frequentation(df_raw):
    """
    Parse la fréquentation, calcule l'évolution 2019→2024.
    Indicateur clé : récupération post-COVID.
    """
    return (
        df_raw
        .select(F.from_json("json_str", SCHEMA_FREQUENTATION).alias("d"))
        .select(
            F.col("d.nom_gare").alias("nom_gare"),
            F.col("d.code_uic_complet").alias("code_uic"),
            F.col("d.code_postal").alias("code_postal"),
            F.col("d.segmentation_drg").alias("segment_drg"),
            F.col("d.total_voyageurs_2024").alias("voyageurs_2024"),
            F.col("d.total_voyageurs_2023").alias("voyageurs_2023"),
            F.col("d.total_voyageurs_2022").alias("voyageurs_2022"),
            F.col("d.total_voyageurs_2019").alias("voyageurs_2019"),
            F.col("d.ingested_at").alias("ingested_at"),
        )
        .withColumn("evolution_pct_2019_2024",
            F.when(
                F.col("voyageurs_2019") > 0,
                F.round(
                    (F.col("voyageurs_2024") - F.col("voyageurs_2019"))
                    / F.col("voyageurs_2019") * 100,
                    1
                )
            ).otherwise(None)
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def transformer_tgvmax(df_raw):
    """
    Parse les disponibilités TGV MAX.
    Calcule est_disponible, OD, jour semaine, weekend, CO2 économisé.
    Facteurs ADEME : voiture=192 gCO2/km, TGV=2.5 gCO2/km.
    """
    CO2_VOITURE = 192.0
    CO2_TRAIN   = 2.5

    return (
        df_raw
        .select(F.from_json("json_str", SCHEMA_TGVMAX).alias("d"))
        .select(
            F.col("d.date").alias("date_depart"),
            F.col("d.train_no").alias("train_no"),
            F.col("d.axe").alias("axe"),
            F.col("d.origine").alias("origine"),
            F.col("d.destination").alias("destination"),
            F.col("d.origine_iata").alias("origine_iata"),
            F.col("d.destination_iata").alias("destination_iata"),
            F.col("d.heure_depart").alias("heure_depart"),
            F.col("d.heure_arrivee").alias("heure_arrivee"),
            F.col("d.od_happy_card").alias("od_happy_card"),
            F.col("d.ingested_at").alias("ingested_at"),
        )
        .withColumn("est_disponible",
            F.col("od_happy_card") == "OUI"
        )
        .withColumn("od",
            F.concat(F.col("origine"), F.lit("->"), F.col("destination"))
        )
        .withColumn("jour_semaine",
            F.dayofweek(F.to_date("date_depart", "yyyy-MM-dd")) - 2
        )
        .withColumn("est_weekend",
            F.col("jour_semaine").isin([5, 6])
        )
        .withColumn("co2_economise_kg",
            F.round(
                F.lit(580.0) * (F.lit(CO2_VOITURE) - F.lit(CO2_TRAIN)) / 1000,
                1
            )
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def transformer_regularite(df_raw):
    """
    Parse la régularité mensuelle TGV.
    Calcule taux de régularité et taux d'annulation.
    """
    return (
        df_raw
        .select(F.from_json("json_str", SCHEMA_REGULARITE).alias("d"))
        .select(
            F.col("d.date").alias("periode"),
            F.col("d.gare_depart").alias("gare_depart"),
            F.col("d.gare_arrivee").alias("gare_arrivee"),
            F.col("d.nb_train_prevu").alias("nb_train_prevu"),
            F.col("d.nb_annulation").alias("nb_annulation"),
            F.col("d.nb_train_retard_arrivee").alias("nb_train_retard"),
            F.col("d.retard_moyen_arrivee").alias("retard_moyen_min"),
            F.col("d.nb_train_retard_sup_15").alias("nb_retard_sup_15"),
            F.col("d.nb_train_retard_sup_30").alias("nb_retard_sup_30"),
            F.col("d.prct_cause_externe").alias("pct_cause_externe"),
            F.col("d.prct_cause_infra").alias("pct_cause_infra"),
            F.col("d.prct_cause_materiel_roulant").alias("pct_cause_materiel"),
            F.col("d.ingested_at").alias("ingested_at"),
        )
        .withColumn("taux_regularite_pct",
            F.when(
                F.col("nb_train_prevu") > 0,
                F.round(
                    (F.col("nb_train_prevu")
                     - F.col("nb_annulation")
                     - F.col("nb_train_retard"))
                    / F.col("nb_train_prevu") * 100,
                    1
                )
            ).otherwise(None)
        )
        .withColumn("taux_annulation_pct",
            F.when(
                F.col("nb_train_prevu") > 0,
                F.round(
                    F.col("nb_annulation")
                    / F.col("nb_train_prevu") * 100,
                    1
                )
            ).otherwise(None)
        )
        .withColumn("processed_at", F.current_timestamp())
    )


# ─────────────────────────────────────────────────────────────────
# ÉCRITURE VERS MINIO (FORMAT PARQUET)
# ─────────────────────────────────────────────────────────────────

def ecrire_silver(df, chemin: str, checkpoint: str, partition_col: str = None):
    """
    Écrit un DataFrame streaming en Parquet dans MinIO via foreachBatch.
    Plus robuste sur Windows que le writer Parquet natif.

    foreachBatch reçoit chaque micro-batch comme un DataFrame statique
    → on peut utiliser le writer Parquet classique sans problème NativeIO.
    """
    def write_batch(batch_df, batch_id):
        # Ne rien faire si le batch est vide
        if batch_df.isEmpty():
            return
        count = batch_df.count()
        writer = batch_df.write.format("parquet").mode("append")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.save(chemin)
        print(f"  Batch {batch_id} → {chemin} ({count} lignes)")

    return (
        df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ─────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE PRINCIPAL
# ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Spark Streaming — SNCF Tourisme en Train")
    print("=" * 60)

    spark = creer_spark_session()

    # ── Lecture des topics Kafka ─────────────────────────────────
    print("\nLecture des topics Kafka...")
    raw_gares         = lire_topic_kafka(spark, TOPICS["gares"])
    raw_frequentation = lire_topic_kafka(spark, TOPICS["frequentation"])
    raw_tgvmax        = lire_topic_kafka(spark, TOPICS["tgvmax"])
    raw_regularite    = lire_topic_kafka(spark, TOPICS["regularite"])

    # ── Transformations Silver ───────────────────────────────────
    print("Application des transformations Silver...")
    silver_gares         = transformer_gares(raw_gares)
    silver_frequentation = transformer_frequentation(raw_frequentation)
    silver_tgvmax        = transformer_tgvmax(raw_tgvmax)
    silver_regularite    = transformer_regularite(raw_regularite)

    # ── Écriture dans MinIO ──────────────────────────────────────
    print("Démarrage des streams d'écriture vers MinIO...")

    q1 = ecrire_silver(
        silver_gares,
        chemin     = f"{SILVER_BASE}/gares",
        checkpoint = f"{CHECKPOINT_BASE}/gares"
    )
    q2 = ecrire_silver(
        silver_frequentation,
        chemin     = f"{SILVER_BASE}/frequentation",
        checkpoint = f"{CHECKPOINT_BASE}/frequentation"
    )
    q3 = ecrire_silver(
        silver_tgvmax,
        chemin        = f"{SILVER_BASE}/tgvmax",
        checkpoint    = f"{CHECKPOINT_BASE}/tgvmax",
        partition_col = "date_depart"
    )
    q4 = ecrire_silver(
        silver_regularite,
        chemin        = f"{SILVER_BASE}/regularite",
        checkpoint    = f"{CHECKPOINT_BASE}/regularite",
        partition_col = "periode"
    )

    print("\nStreams actifs — écriture en cours dans MinIO...")
    print("Ctrl+C pour arrêter proprement.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
        q1.stop()
        q2.stop()
        q3.stop()
        q4.stop()
        print("Streams arrêtés proprement.")

    spark.stop()
    print("Session Spark fermée.")


if __name__ == "__main__":
    main()