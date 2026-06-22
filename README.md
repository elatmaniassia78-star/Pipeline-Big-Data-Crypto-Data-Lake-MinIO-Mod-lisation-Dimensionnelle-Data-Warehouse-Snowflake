# Pipeline Big Data Crypto – Data Lake MinIO, Modélisation Dimensionnelle, Snowflake & Tableau

## 📌 Contexte du projet

Le marché des cryptomonnaies génère chaque jour des volumes massifs de données : prix, volumes d’échange, capitalisations boursières et indicateurs de performance. Ces données représentent une source précieuse d’information pour les investisseurs et les analystes.

L’objectif de ce projet est de concevoir et implémenter une architecture Data complète permettant de collecter, stocker, transformer et analyser les données crypto en suivant les bonnes pratiques de la Data Engineering moderne.

Le projet couvre l’ensemble de la chaîne de valeur :

* Ingestion des données depuis l’API CoinGecko
* Stockage dans un Data Lake MinIO selon l’architecture Medallion
* Modélisation dimensionnelle des données
* Chargement dans Snowflake
* Orchestration avec Apache Airflow
* Visualisation et analyse avec Tableau

---

# 🏗 Architecture du projet

## Architecture Medallion

Le pipeline suit une architecture Medallion composée de trois couches :

### Bronze Layer

Données brutes provenant de l’API CoinGecko.

Format :

```text
JSON
```

Stockage :

```text
crypto-bronze/YYYY/MM/DD/raw.json
```

Objectif :

* Conserver les données originales
* Permettre le rejeu des traitements
* Garantir la traçabilité

---

### Silver Layer

Données nettoyées et standardisées.

Transformations :

* Nettoyage des valeurs
* Normalisation des noms de colonnes
* Conversion au format snake_case
* Contrôle qualité

Format :

```text
Parquet
```

Stockage :

```text
crypto-silver/
```

---

### Gold Layer

Implémentation du modèle dimensionnel.

Contenu :

* Tables de dimensions
* Tables de faits

Format :

```text
Parquet
```

Stockage :

```text
crypto-gold/
```

---

## Flux global

```text
CoinGecko API
       │
       ▼
 Bronze Layer (JSON)
       │
       ▼
 Silver Layer (Parquet)
       │
       ▼
 Gold Layer (Dimension Model)
       │
       ▼
 Snowflake Data Warehouse
       │
       ▼
 Tableau Dashboard
```

---

# 🗄 Modélisation Dimensionnelle

## Processus métier analysés

* Évolution des prix
* Volumes échangés
* Capitalisation boursière
* Performance des cryptomonnaies

## Granularité

Chaque ligne de la table de faits représente :

```text
Une cryptomonnaie à une date et heure de collecte donnée
```

## Schéma choisi

Star Schema

---

## Table de dimension : DIM_CRYPTO

| Colonne         | Type    |
| --------------- | ------- |
| COIN_ID         | VARCHAR |
| NAME            | VARCHAR |
| SYMBOL          | VARCHAR |
| MARKET_CAP_RANK | INTEGER |

Clé primaire :

```text
COIN_ID
```

---

## Table de dimension : DIM_DATE

| Colonne | Type    |
| ------- | ------- |
| DATE_ID | DATE    |
| YEAR    | INTEGER |
| MONTH   | INTEGER |
| WEEK    | INTEGER |
| DAY     | INTEGER |

Clé primaire :

```text
DATE_ID
```

---

## Table de faits : FACT_CRYPTO_SNAPSHOT

| Colonne                     | Type      |
| --------------------------- | --------- |
| COIN_ID                     | VARCHAR   |
| DATE_ID                     | DATE      |
| CURRENT_PRICE               | FLOAT     |
| HIGH_24H                    | FLOAT     |
| LOW_24H                     | FLOAT     |
| TOTAL_VOLUME                | FLOAT     |
| MARKET_CAP                  | FLOAT     |
| PRICE_CHANGE_24H            | FLOAT     |
| PRICE_CHANGE_PERCENTAGE_24H | FLOAT     |
| COLLECTED_AT                | TIMESTAMP |

Clés étrangères :

```text
COIN_ID → DIM_CRYPTO
DATE_ID → DIM_DATE
```

---

# ⚙ Pipeline ETL

## Étape 1 — Ingestion Bronze

* Connexion à l’API CoinGecko
* Collecte des données crypto
* Sauvegarde JSON dans MinIO
* Gestion des erreurs HTTP et Timeouts

---

## Étape 2 — Transformation Silver

* Lecture des fichiers JSON
* Nettoyage avec Pandas
* Standardisation des colonnes
* Sauvegarde Parquet

---

## Étape 3 — Construction Gold

* Création des dimensions
* Création des tables de faits
* Contrôle d’intégrité référentielle
* Sauvegarde dans MinIO

---

## Étape 4 — Chargement Snowflake

* Connexion Snowflake
* Création du schéma analytique
* Chargement des dimensions
* Chargement des faits
* Validation des données

---

## Étape 5 — Orchestration Airflow

DAG principal :

```text
cryptopipelinedag
```

Ordre d’exécution :

```text
ingestbronze
      ↓
transformsilver
      ↓
buildgoldmodel
      ↓
load_snowflake
```

Fonctionnalités :

* Scheduling quotidien
* Retries automatiques
* Monitoring
* Gestion des erreurs

---

# 📊 Visualisation Tableau

## Dashboard Principal

Vue comparative multi-cryptos :

* KPI Cards
* Evolution des prix
* Top 10 cryptos par volume
* Heatmap des performances
* Scatter Plot volume/prix
* Filtres globaux

---

## Dashboard Détail

Analyse approfondie d’une cryptomonnaie :

* Historique complet des prix
* Volume échangé
* Capitalisation
* Variation 24h
* Profil détaillé de la crypto

Navigation :

```text
Dashboard Principal
        ↓
Sélection d'une crypto
        ↓
Dashboard Détail
```

---

# 🛠 Technologies utilisées

* Python
* Pandas
* CoinGecko API
* MinIO
* Apache Airflow
* Snowflake
* Tableau
* Docker
* Git & GitHub

---

# 🚀 Résultats

Ce projet met en œuvre un pipeline Big Data complet permettant :

* L’ingestion automatisée de données crypto
* Le stockage dans un Data Lake Medallion
* La modélisation dimensionnelle des données
* L’alimentation d’un Data Warehouse Snowflake
* L’orchestration avec Airflow
* La visualisation interactive avec Tableau

Le résultat final fournit une plateforme analytique permettant de suivre et comparer les performances des cryptomonnaies de manière fiable et scalable.
