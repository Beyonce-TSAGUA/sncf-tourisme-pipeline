# Tourisme en Train — Pipeline de données SNCF

Projet réalisé dans le cadre du défi [Tourisme en Train](https://defis.data.gouv.fr/defis/tourisme-en-train) 
sur data.gouv.fr — Région Nouvelle-Aquitaine.

## Objectif

Répondre à la question : **les territoires touristiques de Nouvelle-Aquitaine 
sont-ils bien desservis par le train ?**

Le pipeline collecte, transforme et visualise en temps réel les données 
ferroviaires et touristiques open data pour produire un dashboard Power BI.

## Architecture
```
API SNCF (temps réel)
        ↓
    Kafka (streaming)
        ↓
  Spark Structured Streaming
        ↓
  MinIO (Parquet — couche Silver)
        ↓
    Power BI (dashboard)

Orchestration : Apache Airflow
```

## Données utilisées

| Source | Contenu | Type |
|--------|---------|------|
| API SNCF open data | Gares, horaires, fréquentation | Temps réel |
| DATAtourisme | POI touristiques Nouvelle-Aquitaine | Statique |
| data.gouv.fr | Aménagements cyclables | Statique |
| ADEME Base Carbone | Facteurs d'émission CO₂ | Statique |

## Lancer le projet

### Prérequis
- Docker Desktop
- Python 3.10+

### Démarrage
```bash
# 1. Copier et remplir les variables d'environnement
cp .env.example .env

# 2. Lancer toute l'infrastructure
docker compose up -d

# 3. Lancer le producer Kafka
python producers/sncf_producer.py

# 4. Accès aux interfaces
# Airflow   → http://localhost:8080
# MinIO     → http://localhost:9001
```

## Questions auxquelles répond le dashboard

- Quelles gares sont proches de sites touristiques ?
- Quelles liaisons ont des places TGV MAX disponibles ce weekend ?
- Combien de CO₂ économise-t-on en prenant le train vs la voiture ?
- Quelles zones touristiques sont accessibles à vélo depuis une gare ?