"""
=====================================================================
Producer Kafka — Données SNCF Open Data
=====================================================================
Rôle :
    Ce script récupère les données ferroviaires depuis l'API open
    data SNCF et les publie dans des topics Kafka pour être
    consommées en temps réel par le job Spark.

Flux :
    API SNCF → fetch → Kafka topics

Topics Kafka produits :
    - sncf-gares          : gares de Nouvelle-Aquitaine (GPS, nom)
    - sncf-frequentation  : nb voyageurs par gare (2015→2024)
    - sncf-tgvmax         : disponibilités TGV MAX (30j glissants)
    - sncf-regularite     : régularité mensuelle TGV par axe

Utilisation :
    python producers/sncf_producer.py

Prérequis :
    - Kafka doit tourner (docker compose up -d)
    - Variables d'environnement dans .env
=====================================================================
"""

import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# ─────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Topics Kafka — un par type de données
TOPIC_GARES         = "sncf-gares"
TOPIC_FREQUENTATION = "sncf-frequentation"
TOPIC_TGVMAX        = "sncf-tgvmax"
TOPIC_REGULARITE    = "sncf-regularite"

# URL de base de l'API open data SNCF
API_BASE = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets"

# Bounding box géographique Nouvelle-Aquitaine
# Toutes les gares dont les coordonnées GPS tombent dans cette zone
LAT_MIN, LAT_MAX = 43.0, 47.0
LON_MIN, LON_MAX = -2.0,  2.5

# Axes TGV qui passent par la Nouvelle-Aquitaine
# Utilisé pour filtrer la régularité
AXES_NAQ = ["ATLANTIQUE", "SUD OUEST", "BORDEAUX"]


# ─────────────────────────────────────────────────────────────────
# INITIALISATION DU PRODUCER KAFKA
# ─────────────────────────────────────────────────────────────────

def creer_producer():
    """
    Crée et retourne un producer Kafka configuré.

    - value_serializer : convertit les dicts Python en JSON bytes
    - key_serializer   : la clé permet à Kafka de partitionner
    - retries          : réessaie en cas d'échec réseau
    """
    print(f"Connexion au broker Kafka : {KAFKA_BROKER}")
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3,
        request_timeout_ms=30000,
    )


# ─────────────────────────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ─────────────────────────────────────────────────────────────────

def ajouter_timestamp(record: dict) -> dict:
    """
    Ajoute un champ ingested_at à chaque enregistrement.
    Utile pour Spark qui va horodater chaque message reçu.
    """
    record["ingested_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return record


def fetch_tous_les_records(dataset_id: str, params_extra: dict = None) -> list:
    """
    Récupère TOUS les enregistrements d'un dataset en paginant
    automatiquement (l'API limite à 100 par requête).

    Paramètres :
        dataset_id  : identifiant du dataset SNCF
        params_extra: paramètres additionnels (where, refine, etc.)

    Retourne : liste complète de tous les enregistrements
    """
    url = f"{API_BASE}/{dataset_id}/records"
    tous = []
    offset = 0
    limit = 100

    while True:
        params = {"limit": limit, "offset": offset}
        if params_extra:
            params.update(params_extra)

        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        resultats = data.get("results", [])
        if not resultats:
            break

        tous.extend(resultats)
        offset += limit

        if offset >= data.get("total_count", 0):
            break

        time.sleep(0.3)  # pause pour respecter le rate limit

    return tous


# ─────────────────────────────────────────────────────────────────
# FONCTIONS DE RÉCUPÉRATION PAR DATASET
# ─────────────────────────────────────────────────────────────────

def fetch_gares() -> list:
    """
    Récupère les gares SNCF et filtre localement celles
    qui sont en Nouvelle-Aquitaine via leur bounding box GPS.

    Dataset : gares-de-voyageurs
    Champs  : nom, libellecourt, codes_uic, position_geographique,
              segment_drg, codeinsee

    Filtre GPS NAQ :
        latitude  43.0 → 47.0
        longitude -2.0 → 2.5
    """
    print("  Appel API : gares-de-voyageurs")
    tous = fetch_tous_les_records("gares-de-voyageurs")

    gares_naq = []
    for gare in tous:
        pos = gare.get("position_geographique", {})
        lat = pos.get("lat")
        lon = pos.get("lon")
        if lat and lon:
            if LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX:
                gares_naq.append(gare)

    return gares_naq


def fetch_frequentation() -> list:
    """
    Récupère la fréquentation annuelle de toutes les gares.
    On filtre ensuite sur les gares NAQ via code_uic_complet.

    Dataset : frequentation-gares
    Champs  : nom_gare, code_uic_complet, code_postal,
              segmentation_drg, total_voyageurs_2024,
              total_voyageurs_2023 ... (2015→2024)

    Note : ce dataset contient toutes les gares France.
    On le croise avec les gares NAQ via code_uic_complet.
    """
    print("  Appel API : frequentation-gares")
    return fetch_tous_les_records("frequentation-gares")


def fetch_tgvmax() -> list:
    """
    Récupère les disponibilités TGV MAX sur les 30 prochains jours.
    C'est notre principale source de données temps réel.

    Dataset : tgvmax
    Champs  : date, train_no, axe, origine, destination,
              heure_depart, heure_arrivee, od_happy_card

    od_happy_card = 'OUI' → place disponible pour abonné MAX

    Note : l'API bloque au-delà de 10 000 enregistrements.
    On filtre sur l'axe ATLANTIQUE ET les dates futures
    pour rester dans cette limite.
    """
    print("  Appel API : tgvmax")
    today = time.strftime("%Y-%m-%d", time.gmtime())

    url = f"{API_BASE}/tgvmax/records"
    tous = []
    offset = 0
    limit = 100

    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "where": f"axe='ATLANTIQUE' AND date>='{today}'"
        }

        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        resultats = data.get("results", [])
        if not resultats:
            break

        tous.extend(resultats)
        offset += limit

        total = data.get("total_count", 0)
        if offset >= total or offset >= 5000:  # sécurité max 5000
            break

        time.sleep(0.3)

    return tous


def fetch_regularite() -> list:
    """
    Récupère la régularité mensuelle TGV sur les liaisons
    au départ de Bordeaux Saint-Jean (gare principale NAQ).

    Dataset : regularite-mensuelle-tgv-aqst
    Champs  : date, gare_depart, gare_arrivee, nb_train_prevu,
              nb_annulation, nb_train_retard_arrivee,
              retard_moyen_arrivee, prct_cause_externe,
              prct_cause_infra, prct_cause_materiel_roulant...

    Filtre : gare_depart contient 'BORDEAUX'
    """
    print("  Appel API : regularite-mensuelle-tgv-aqst")
    return fetch_tous_les_records(
        "regularite-mensuelle-tgv-aqst",
        params_extra={"where": "gare_depart like '%BORDEAUX%'"}
    )


# ─────────────────────────────────────────────────────────────────
# FONCTIONS DE PUBLICATION DANS KAFKA
# ─────────────────────────────────────────────────────────────────

def publier(producer, topic: str, records: list, cle_field: str = None):
    """
    Fonction générique de publication dans un topic Kafka.

    Paramètres :
        producer   : instance KafkaProducer
        topic      : nom du topic Kafka cible
        records    : liste de dicts à publier
        cle_field  : nom du champ à utiliser comme clé Kafka
                     (None = pas de clé, Kafka choisit la partition)
    """
    for record in records:
        record = ajouter_timestamp(record)
        cle = str(record.get(cle_field, "")) if cle_field else None
        producer.send(topic, key=cle, value=record)

    producer.flush()
    print(f"  OK : {len(records)} enregistrements → topic '{topic}'")


def publier_gares(producer) -> list:
    """
    Récupère et publie les gares NAQ dans Kafka.
    Retourne la liste des gares pour usage ultérieur.
    Les gares sont quasi-statiques → publiées une seule fois.
    """
    print("\n[GARES] Récupération en cours...")
    try:
        gares = fetch_gares()
        print(f"  {len(gares)} gares trouvées en Nouvelle-Aquitaine")
        # Clé Kafka = codes_uic (identifiant unique de la gare)
        publier(producer, TOPIC_GARES, gares, cle_field="codes_uic")
        return gares
    except Exception as e:
        print(f"  ERREUR : {e}")
        return []


def publier_frequentation(producer, gares_uic: set):
    """
    Récupère la fréquentation nationale et filtre sur les gares NAQ.
    Publie uniquement les gares de Nouvelle-Aquitaine.

    Paramètres :
        gares_uic : ensemble des codes UIC des gares NAQ
                    (pour filtrer les données nationales)
    """
    print("\n[FREQUENTATION] Récupération en cours...")
    try:
        tous = fetch_frequentation()
        # Filtre : on garde uniquement les gares NAQ
        naq = [
            r for r in tous
            if r.get("code_uic_complet", "") in gares_uic
        ]
        print(f"  {len(naq)}/{len(tous)} gares NAQ trouvées")
        publier(producer, TOPIC_FREQUENTATION, naq, cle_field="code_uic_complet")
    except Exception as e:
        print(f"  ERREUR : {e}")


def publier_tgvmax(producer):
    """
    Récupère et publie les disponibilités TGV MAX.
    Appelé en boucle toutes les 60s → simule le temps réel.

    od_happy_card = 'OUI' signifie qu'une place est disponible
    sur cette liaison à cette date pour les abonnés TGV MAX.
    """
    print("\n[TGVMAX] Récupération en cours...")
    try:
        records = fetch_tgvmax()
        print(f"  {len(records)} disponibilités trouvées (axe Atlantique)")
        # Clé = train_no + date pour éviter les doublons dans Kafka
        for record in records:
            record = ajouter_timestamp(record)
            cle = f"{record.get('train_no','')}_{record.get('date','')}"
            producer.send(TOPIC_TGVMAX, key=cle, value=record)
        producer.flush()
        print(f"  OK : {len(records)} enregistrements → topic '{TOPIC_TGVMAX}'")
    except Exception as e:
        print(f"  ERREUR : {e}")


def publier_regularite(producer):
    """
    Récupère et publie la régularité mensuelle TGV.
    Données historiques → publiées une seule fois au démarrage.
    Utile pour Power BI : courbe d'évolution de la ponctualité.
    """
    print("\n[REGULARITE] Récupération en cours...")
    try:
        records = fetch_regularite()
        print(f"  {len(records)} mois de données de régularité")
        publier(producer, TOPIC_REGULARITE, records, cle_field="periode")
    except Exception as e:
        print(f"  ERREUR : {e}")


# ─────────────────────────────────────────────────────────────────
# POINT D'ENTRÉE PRINCIPAL
# ─────────────────────────────────────────────────────────────────

def main():
    """
    Point d'entrée du producer.

    Logique :
        1. Connexion Kafka
        2. Publication initiale (données quasi-statiques) :
               gares, fréquentation, régularité
        3. Boucle infinie toutes les 60s :
               disponibilités TGV MAX (données temps réel)
    """
    print("=" * 60)
    print("  Producer SNCF — Tourisme en Train")
    print("  Nouvelle-Aquitaine")
    print("=" * 60)

    # Connexion Kafka
    try:
        producer = creer_producer()
    except Exception as e:
        print(f"ERREUR FATALE : impossible de se connecter à Kafka : {e}")
        print("Vérifiez que Docker est bien lancé (docker compose up -d)")
        return

    # ── Publication initiale (une seule fois au démarrage) ──────
    gares = publier_gares(producer)

    # On extrait les codes UIC des gares NAQ pour filtrer
    # la fréquentation (dataset national)
    gares_uic = {g.get("codes_uic", "") for g in gares}

    publier_frequentation(producer, gares_uic)
    publier_regularite(producer)

    if not gares:
        print("\nAVERTISSEMENT : aucune gare récupérée.")

    # ── Boucle temps réel : TGV MAX toutes les 60s ───────────────
    print("\nDémarrage de la boucle temps réel (Ctrl+C pour arrêter)...")
    cycle = 1

    while True:
        try:
            print(f"\n{'─'*40}")
            print(f"Cycle #{cycle}")
            publier_tgvmax(producer)
            print(f"Prochain cycle dans 60 secondes...")
            time.sleep(60)
            cycle += 1

        except KeyboardInterrupt:
            print("\nArrêt demandé.")
            break
        except Exception as e:
            print(f"ERREUR temporaire : {e} — retry dans 30s")
            time.sleep(30)

    producer.close()
    print("Producer arrêté proprement.")


if __name__ == "__main__":
    main()