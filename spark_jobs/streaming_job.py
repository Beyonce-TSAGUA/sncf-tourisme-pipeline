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
    - Nettoyage : doublons, nulls, valeurs négatives, taux impossibles
    - Typage fort : Long, Integer, Double, Date, Boolean
    - Enrichissement : geo_cell, évolution COVID, CO2, taux calculés
    - Écriture Parquet mode overwrite dans MinIO

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

# Checkpoint local Windows
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
    StructField("nom_gare",             StringType(),  True),
    StructField("code_uic_complet",     StringType(),  True),
    StructField("code_postal",          StringType(),  True),
    StructField("segmentation_drg",     StringType(),  True),
    StructField("total_voyageurs_2024", DoubleType(),  True),
    StructField("total_voyageurs_2023", DoubleType(),  True),
    StructField("total_voyageurs_2022", DoubleType(),  True),
    StructField("total_voyageurs_2019", DoubleType(),  True),
    StructField("ingested_at",          StringType(),  True),
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
    - Blocs S3A en mémoire (évite NativeIO Windows)
    - Checkpoint local (évite NativeIO Windows)
    """
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

    print("Démarrage de la session Spark...")

    spark = (
        SparkSession.builder
        .appName("SNCF-Tourisme-Streaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        # ── Configuration MinIO ──────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",         MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",         MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # ── Fix NativeIO Windows ─────────────────────────────────
        .config("spark.hadoop.fs.s3a.fast.upload",        "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.multipart.size",     "67108864")
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
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────────

def lire_topic_kafka(spark: SparkSession, topic: str):
    """
    Lit un topic Kafka en mode streaming.
    Retourne un DataFrame avec colonne 'json_str'.
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
# TRANSFORMATIONS SILVER
# ─────────────────────────────────────────────────────────────────

def transformer_gares(df_raw):
    """
    Nettoyage Silver des gares :
    - Déduplication sur codes_uic
    - Suppression des gares sans GPS
    - Validation bbox France (lat 41-52, lon -5.5 à 10)
    - Typage fort lat/lon en Double
    - Normalisation nom en initcap
    - Calcul geo_cell pour jointures spatiales
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
            F.col("d.position_geographique.lat").cast("double").alias("latitude"),
            F.col("d.position_geographique.lon").cast("double").alias("longitude"),
            F.col("d.ingested_at").alias("ingested_at"),
        )
        # Supprime les nulls critiques
        .filter(
            F.col("latitude").isNotNull() &
            F.col("longitude").isNotNull() &
            F.col("codes_uic").isNotNull()
        )
        # Validation bbox France
        .filter(
            (F.col("latitude")  >= 41)   & (F.col("latitude")  <= 52) &
            (F.col("longitude") >= -5.5) & (F.col("longitude") <= 10)
        )
        # Déduplication sur la clé métier
        .dropDuplicates(["codes_uic"])
        # Normalisation du nom
        .withColumn("nom_gare", F.initcap(F.col("nom_gare")))
        # Geo cell pour jointures spatiales ~10km
        .withColumn("lat_grid", F.round("latitude",  2))
        .withColumn("lon_grid", F.round("longitude", 2))
        .withColumn("geo_cell",
            F.concat(
                F.col("lat_grid").cast("string"),
                F.lit("_"),
                F.col("lon_grid").cast("string")
            )
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def transformer_frequentation(df_raw):
    """
    Nettoyage Silver de la fréquentation :
    - Déduplication sur code_uic
    - Suppression des nulls critiques
    - Remplacement valeurs négatives par 0
    - Remplacement nulls voyageurs par 0
    - Typage fort en Long (entier 64 bits)
    - Calcul évolution COVID 2019→2024
    """
    return (
        df_raw
        .select(F.from_json("json_str", SCHEMA_FREQUENTATION).alias("d"))
        .select(
            F.col("d.nom_gare").alias("nom_gare"),
            F.col("d.code_uic_complet").alias("code_uic"),
            F.col("d.code_postal").alias("code_postal"),
            F.col("d.segmentation_drg").alias("segment_drg"),
            F.col("d.total_voyageurs_2024").cast("long").alias("voyageurs_2024"),
            F.col("d.total_voyageurs_2023").cast("long").alias("voyageurs_2023"),
            F.col("d.total_voyageurs_2022").cast("long").alias("voyageurs_2022"),
            F.col("d.total_voyageurs_2019").cast("long").alias("voyageurs_2019"),
            F.col("d.ingested_at").alias("ingested_at"),
        )
        # Supprime les nulls critiques
        .filter(
            F.col("nom_gare").isNotNull() &
            F.col("code_uic").isNotNull()
        )
        # Déduplication sur la clé métier
        .dropDuplicates(["code_uic"])
        # Normalisation nom
        .withColumn("nom_gare", F.initcap(F.col("nom_gare")))
        # Remplace négatifs et nulls par 0
        .withColumn("voyageurs_2024",
            F.when(F.col("voyageurs_2024") < 0, F.lit(0))
            .otherwise(F.coalesce(F.col("voyageurs_2024"), F.lit(0)))
        )
        .withColumn("voyageurs_2023",
            F.when(F.col("voyageurs_2023") < 0, F.lit(0))
            .otherwise(F.coalesce(F.col("voyageurs_2023"), F.lit(0)))
        )
        .withColumn("voyageurs_2022",
            F.when(F.col("voyageurs_2022") < 0, F.lit(0))
            .otherwise(F.coalesce(F.col("voyageurs_2022"), F.lit(0)))
        )
        .withColumn("voyageurs_2019",
            F.when(F.col("voyageurs_2019") < 0, F.lit(0))
            .otherwise(F.coalesce(F.col("voyageurs_2019"), F.lit(0)))
        )
        # Évolution COVID uniquement si 2019 > 0
        .withColumn("evolution_pct_2019_2024",
            F.when(
                F.col("voyageurs_2019") > 0,
                F.round(
                    (F.col("voyageurs_2024") - F.col("voyageurs_2019"))
                    / F.col("voyageurs_2019") * 100,
                    1
                )
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def transformer_tgvmax(df_raw):
    """
    Nettoyage Silver des disponibilités TGV MAX :
    - Déduplication sur train_no + date + origine + destination
    - Suppression des nulls critiques
    - Garde uniquement les dates futures
    - Typage date en DateType et booléen
    - Calcul jour semaine, weekend, CO2 économisé
    """
    CO2_VOITURE = 192.0  # gCO2/km (voiture thermique — ADEME)
    CO2_TRAIN   = 2.5    # gCO2/km (TGV électrique — ADEME)

    return (
        df_raw
        .select(F.from_json("json_str", SCHEMA_TGVMAX).alias("d"))
        .select(
            F.to_date(F.col("d.date"), "yyyy-MM-dd").alias("date_depart"),
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
        # Supprime les nulls critiques
        .filter(
            F.col("train_no").isNotNull() &
            F.col("date_depart").isNotNull() &
            F.col("origine").isNotNull() &
            F.col("destination").isNotNull()
        )
        # Garde uniquement les dates futures ou aujourd'hui
        .filter(F.col("date_depart") >= F.current_date())
        # Déduplication sur la clé métier
        .dropDuplicates(["train_no", "date_depart", "origine", "destination"])
        # Disponibilité en booléen
        .withColumn("est_disponible",
            F.col("od_happy_card") == "OUI"
        )
        # Paire Origine-Destination
        .withColumn("od",
            F.concat(F.col("origine"), F.lit("->"), F.col("destination"))
        )
        # Jour de semaine (0=lundi, 6=dimanche)
        .withColumn("jour_semaine",
            F.dayofweek(F.col("date_depart")) - 2
        )
        # Flag weekend
        .withColumn("est_weekend",
            F.col("jour_semaine").isin([5, 6])
        )
        # CO2 économisé vs voiture (Bordeaux-Paris ~580km)
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
    Nettoyage Silver de la régularité :
    - Déduplication sur periode + gare_depart + gare_arrivee
    - Suppression des nulls critiques
    - Suppression des lignes avec nb_train_prevu = 0
    - Typage fort Integer et Double
    - Remplacement nulls compteurs par 0
    - Calcul taux régularité et annulation
    - Validation : taux entre 0 et 100
    - Extraction année et mois
    """
    return (
        df_raw
        .select(F.from_json("json_str", SCHEMA_REGULARITE).alias("d"))
        .select(
            F.col("d.date").alias("periode"),
            F.col("d.gare_depart").alias("gare_depart"),
            F.col("d.gare_arrivee").alias("gare_arrivee"),
            F.col("d.nb_train_prevu").cast("integer").alias("nb_train_prevu"),
            F.col("d.nb_annulation").cast("integer").alias("nb_annulation"),
            F.col("d.nb_train_retard_arrivee").cast("integer").alias("nb_train_retard"),
            F.col("d.retard_moyen_arrivee").cast("double").alias("retard_moyen_min"),
            F.col("d.nb_train_retard_sup_15").cast("integer").alias("nb_retard_sup_15"),
            F.col("d.nb_train_retard_sup_30").cast("integer").alias("nb_retard_sup_30"),
            F.col("d.prct_cause_externe").cast("double").alias("pct_cause_externe"),
            F.col("d.prct_cause_infra").cast("double").alias("pct_cause_infra"),
            F.col("d.prct_cause_materiel_roulant").cast("double").alias("pct_cause_materiel"),
            F.col("d.ingested_at").alias("ingested_at"),
        )
        # Supprime les nulls critiques
        .filter(
            F.col("periode").isNotNull() &
            F.col("gare_depart").isNotNull() &
            F.col("nb_train_prevu").isNotNull()
        )
        # Supprime les lignes sans trains prévus (évite division par zéro)
        .filter(F.col("nb_train_prevu") > 0)
        # Déduplication sur la clé métier
        .dropDuplicates(["periode", "gare_depart", "gare_arrivee"])
        # Remplace les nulls des compteurs par 0
        .fillna(0, subset=[
            "nb_annulation", "nb_train_retard",
            "nb_retard_sup_15", "nb_retard_sup_30"
        ])
        # Taux de régularité
        .withColumn("taux_regularite_pct",
            F.round(
                (F.col("nb_train_prevu")
                 - F.col("nb_annulation")
                 - F.col("nb_train_retard"))
                / F.col("nb_train_prevu") * 100,
                1
            )
        )
        # Validation : supprime les taux impossibles
        .filter(
            (F.col("taux_regularite_pct") >= 0) &
            (F.col("taux_regularite_pct") <= 100)
        )
        # Taux d'annulation
        .withColumn("taux_annulation_pct",
            F.round(
                F.col("nb_annulation") / F.col("nb_train_prevu") * 100,
                1
            )
        )
        # Extraction année et mois pour les visuels temporels
        .withColumn("annee", F.col("periode").substr(1, 4))
        .withColumn("mois",  F.col("periode").substr(6, 2))
        .withColumn("processed_at", F.current_timestamp())
    )


# ─────────────────────────────────────────────────────────────────
# ÉCRITURE VERS MINIO (FORMAT PARQUET)
# ─────────────────────────────────────────────────────────────────

def ecrire_silver(df, chemin: str, checkpoint: str,
                  partition_col: str = None, mode: str = "overwrite"):
    """
    Écrit un DataFrame streaming en Parquet dans MinIO via foreachBatch.

    mode='overwrite' : écrase les données à chaque batch
                       → garantit des données propres sans doublons
                       → adapté aux données quasi-statiques

    mode='append'    : accumule les données batch après batch
                       → adapté uniquement si on veut l'historique

    foreachBatch est plus robuste sur Windows que le writer natif.
    """
    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        # Déduplication finale de sécurité
        batch_df = batch_df.dropDuplicates()
        count = batch_df.count()
        writer = batch_df.write.format("parquet").mode(mode)
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.save(chemin)
        print(f"  Batch {batch_id} → {chemin} ({count} lignes, mode={mode})")

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

    # ── Écriture dans MinIO (mode overwrite = pas de doublons) ───
    print("Démarrage des streams d'écriture vers MinIO...")

    q1 = ecrire_silver(
        silver_gares,
        chemin     = f"{SILVER_BASE}/gares",
        checkpoint = f"{CHECKPOINT_BASE}/gares",
        mode       = "overwrite"
    )
    q2 = ecrire_silver(
        silver_frequentation,
        chemin     = f"{SILVER_BASE}/frequentation",
        checkpoint = f"{CHECKPOINT_BASE}/frequentation",
        mode       = "overwrite"
    )
    q3 = ecrire_silver(
        silver_tgvmax,
        chemin        = f"{SILVER_BASE}/tgvmax",
        checkpoint    = f"{CHECKPOINT_BASE}/tgvmax",
        partition_col = "date_depart",
        mode          = "overwrite"
    )
    q4 = ecrire_silver(
        silver_regularite,
        chemin        = f"{SILVER_BASE}/regularite",
        checkpoint    = f"{CHECKPOINT_BASE}/regularite",
        partition_col = "periode",
        mode          = "overwrite"
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