"""
=====================================================================
DAG Airflow — Pipeline SNCF Tourisme en Train
=====================================================================
Rôle :
    Orchestration automatique du pipeline de données SNCF.
    Ce DAG planifie et coordonne l'exécution du producer Kafka
    et du job Spark Streaming.

Planning :
    - Exécution quotidienne à 6h UTC
    - Le producer tourne pendant 10 minutes pour collecter
      les données fraîches depuis l'API SNCF
    - Spark transforme et écrit les données en Parquet dans MinIO

Structure du DAG :
    check_services
         ↓
    run_producer (10 minutes)
         ↓
    run_spark_job (traitement Silver)
         ↓
    verify_minio

Utilisation :
    - Accessible via http://localhost:8080
    - DAG id : sncf_tourisme_pipeline
=====================================================================
"""

import subprocess
import os
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ─────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────

# Chemin absolu du projet — Airflow a besoin du chemin complet
PROJECT_DIR = "/opt/airflow"

# Durée de collecte du producer en secondes
# 10 minutes suffisent pour récupérer un cycle complet de données
PRODUCER_DURATION = 600

# Arguments par défaut du DAG
default_args = {
    "owner":            "data-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ─────────────────────────────────────────────────────────────────
# TÂCHES DU DAG
# ─────────────────────────────────────────────────────────────────

def task_check_services():
    """
    Vérifie que Kafka et MinIO sont accessibles avant de démarrer.
    Évite de lancer le producer si l'infrastructure est down.

    Vérifie :
        - Kafka : connexion TCP sur port 29092 (réseau interne Docker)
        - MinIO : requête HTTP sur l'endpoint health

    Lève une exception si un service est inaccessible
    → Airflow marque la tâche en échec et n'exécute pas la suite.
    """
    import socket
    import urllib.request

    # Vérification Kafka (port interne Docker)
    print("Vérification de Kafka...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(("kafka", 29092))
        sock.close()
        if result == 0:
            print("  Kafka : OK")
        else:
            raise Exception("Kafka inaccessible sur kafka:29092")
    except Exception as e:
        raise Exception(f"Kafka inaccessible : {e}")

    # Vérification MinIO
    print("Vérification de MinIO...")
    try:
        urllib.request.urlopen(
            "http://minio:9000/minio/health/live",
            timeout=5
        )
        print("  MinIO : OK")
    except Exception as e:
        raise Exception(f"MinIO inaccessible : {e}")

    print("Tous les services sont opérationnels.")


def task_run_producer():
    """
    Lance le producer SNCF pendant PRODUCER_DURATION secondes.

    Le producer récupère les données depuis l'API SNCF et les
    publie dans les topics Kafka. On le lance en subprocess
    pour pouvoir le terminer proprement après la durée voulue.

    En production, le producer tournerait en continu — ici on
    le limite à 10 minutes pour le pipeline quotidien d'Airflow.
    """
    print(f"Démarrage du producer pour {PRODUCER_DURATION}s...")

    producer_script = os.path.join(PROJECT_DIR, "producers", "sncf_producer.py")

    # Lance le producer en subprocess
    proc = subprocess.Popen(
        ["python", producer_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    # Attend PRODUCER_DURATION secondes en affichant les logs
    start = time.time()
    while time.time() - start < PRODUCER_DURATION:
        line = proc.stdout.readline()
        if line:
            print(line.rstrip())
        if proc.poll() is not None:
            print("Producer terminé prématurément.")
            break
        time.sleep(0.1)

    # Arrête le producer proprement
    if proc.poll() is None:
        proc.terminate()
        proc.wait(timeout=30)
        print("Producer arrêté proprement après timeout.")

    print("Tâche producer terminée.")


def task_run_spark():
    """
    Lance le job Spark Structured Streaming.

    Le job lit les topics Kafka et écrit les données transformées
    en Parquet dans MinIO (couche Silver). On le lance avec un
    trigger "once" pour qu'il traite tous les messages disponibles
    puis s'arrête automatiquement — adapté au pipeline batch.

    En mode streaming continu, on utiliserait processingTime.
    En mode orchestration Airflow, on utilise availableNow
    qui traite tous les messages disponibles puis termine.
    """
    print("Démarrage du job Spark...")

    spark_script = os.path.join(PROJECT_DIR, "spark_jobs", "streaming_job.py")

    proc = subprocess.Popen(
        ["python", spark_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "SPARK_TRIGGER_MODE": "once"}
    )

    # Affiche les logs Spark en temps réel
    for line in proc.stdout:
        print(line.rstrip())

    proc.wait()

    if proc.returncode != 0:
        raise Exception(f"Job Spark échoué avec code {proc.returncode}")

    print("Job Spark terminé avec succès.")


def task_verify_minio():
    """
    Vérifie que les fichiers Parquet ont bien été écrits dans MinIO.

    Utilise boto3 (client S3 compatible MinIO) pour lister les
    objets dans le bucket silver et vérifier qu'ils existent.

    Lève une exception si un dataset est manquant
    → permet de détecter les problèmes d'écriture silencieux.
    """
    import boto3
    from botocore.client import Config

    print("Vérification des données dans MinIO...")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    # Datasets attendus dans le bucket silver
    datasets_attendus = ["gares", "frequentation", "tgvmax", "regularite"]
    datasets_ok = []

    for dataset in datasets_attendus:
        try:
            response = s3.list_objects_v2(
                Bucket="silver",
                Prefix=f"{dataset}/",
                MaxKeys=1
            )
            count = response.get("KeyCount", 0)
            if count > 0:
                print(f"  silver/{dataset}/ : OK ({count} objet(s))")
                datasets_ok.append(dataset)
            else:
                print(f"  silver/{dataset}/ : VIDE - aucun fichier trouvé")
        except Exception as e:
            print(f"  silver/{dataset}/ : ERREUR - {e}")

    if len(datasets_ok) < len(datasets_attendus):
        manquants = set(datasets_attendus) - set(datasets_ok)
        raise Exception(f"Datasets manquants dans MinIO : {manquants}")

    print(f"Vérification OK : {len(datasets_ok)}/{len(datasets_attendus)} datasets présents.")


# ─────────────────────────────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────────────────────────────

with DAG(
    dag_id="sncf_tourisme_pipeline",
    description="Pipeline quotidien SNCF Tourisme en Train — Nouvelle-Aquitaine",
    # Exécution quotidienne à 6h UTC
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    # Ne pas rattraper les exécutions passées
    catchup=False,
    default_args=default_args,
    tags=["sncf", "kafka", "spark", "minio", "silver"],
    # Description affichée dans l'UI Airflow
    doc_md="""
## Pipeline SNCF Tourisme en Train

Ce DAG orchestre le pipeline de données SNCF pour la région
Nouvelle-Aquitaine dans le cadre du défi data.gouv.fr.

### Flux de données
```
API SNCF → Kafka → Spark → MinIO Silver → Power BI
```

### Datasets produits
- `silver/gares` : 463 gares NAQ avec coordonnées GPS
- `silver/frequentation` : voyageurs 2015-2024 par gare
- `silver/tgvmax` : disponibilités TGV MAX 30j glissants
- `silver/regularite` : ponctualité mensuelle axe Atlantique
    """,
) as dag:

    # ── Tâche 1 : Vérification des services ─────────────────────
    check_services = PythonOperator(
        task_id="check_services",
        python_callable=task_check_services,
        doc_md="Vérifie que Kafka et MinIO sont accessibles."
    )

    # ── Tâche 2 : Collecte des données via le producer ──────────
    run_producer = PythonOperator(
        task_id="run_producer",
        python_callable=task_run_producer,
        doc_md="Lance le producer SNCF pendant 10 minutes.",
        execution_timeout=timedelta(minutes=15),
    )

    # ── Tâche 3 : Transformation Spark → MinIO Silver ───────────
    run_spark = PythonOperator(
        task_id="run_spark_job",
        python_callable=task_run_spark,
        doc_md="Lit Kafka, transforme et écrit en Parquet dans MinIO.",
        execution_timeout=timedelta(minutes=30),
    )

    # ── Tâche 4 : Vérification des données dans MinIO ───────────
    verify_minio = PythonOperator(
        task_id="verify_minio",
        python_callable=task_verify_minio,
        doc_md="Vérifie que tous les datasets Silver sont présents.",
    )

    # ── Ordre d'exécution ────────────────────────────────────────
    # check_services → run_producer → run_spark → verify_minio
    check_services >> run_producer >> run_spark >> verify_minio