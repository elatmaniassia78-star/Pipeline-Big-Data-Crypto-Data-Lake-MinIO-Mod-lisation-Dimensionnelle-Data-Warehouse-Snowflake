import pandas as pd
import os
import logging
from datetime import datetime
from io import BytesIO
from minio import Minio
from minio.error import S3Error

# =====================================
# LOGGING CONFIG
# =====================================
logging.basicConfig(
    filename="pipeline_gold.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# =====================================
# MINIO CLIENT
# =====================================
def get_minio_client():
    logger.info("Connexion à MinIO...")
    return Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

# =====================================
# 1. DIMENSIONS
# =====================================

def build_dim_crypto(df):
    logger.info("Construction dim_crypto...")

    dim_crypto = df[["id", "symbol", "name"]].drop_duplicates().copy()
    logger.info(f"dim_crypto après drop_duplicates: {dim_crypto.shape}")

    dim_crypto = dim_crypto.reset_index(drop=True)
    dim_crypto["crypto_key"] = dim_crypto.index + 1

    logger.info(f"dim_crypto final: {len(dim_crypto)} lignes")
    return dim_crypto


def build_dim_date(df):
    logger.info("Construction dim_date...")

    dim_date = df[[
        "date", "year", "month", "day",
        "hour", "minute", "week", "quarter"
    ]].drop_duplicates().copy()

    logger.info(f"dim_date après drop_duplicates: {dim_date.shape}")

    dim_date = dim_date.reset_index(drop=True)
    dim_date["date_key"] = dim_date.index + 1

    logger.info(f"dim_date final: {len(dim_date)} lignes")
    return dim_date

# =====================================
# 2. FACT TABLE
# =====================================

def build_fact_table(df, dim_crypto, dim_date):

    logger.info("Construction fact_table...")

    fact = df.copy()
    logger.info(f"fact initial shape: {fact.shape}")

    # Join crypto
    fact = fact.merge(
        dim_crypto,
        on=["id", "symbol", "name"],
        how="left"
    )
    logger.info("Merge dim_crypto terminé")

    # Join date
    fact = fact.merge(
        dim_date,
        on=["date", "year", "month", "day", "hour", "minute", "week", "quarter"],
        how="left"
    )
    logger.info("Merge dim_date terminé")

    # Integrity check
    missing_crypto = fact["crypto_key"].isna().sum()
    missing_date = fact["date_key"].isna().sum()

    logger.info(f"FK check - missing crypto_key: {missing_crypto}")
    logger.info(f"FK check - missing date_key: {missing_date}")

    if missing_crypto > 0 or missing_date > 0:
        logger.error("❌ Erreur intégrité référentielle détectée")
        raise Exception(
            f"FK non résolues: crypto={missing_crypto}, date={missing_date}"
        )

    fact = fact[[
        "crypto_key",
        "date_key",
        "current_price",
        "market_cap",
        "market_cap_rank",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "circulating_supply",
        "max_supply",
        "ath"
    ]]

    logger.info(f"fact final shape: {fact.shape}")
    logger.info("fact_table construite avec succès")

    return fact

# =====================================
# 3. SAVE GOLD
# =====================================

def save_gold(client, dim_crypto, dim_date, fact):

    logger.info("Début sauvegarde GOLD...")

    bucket = "crypto-gold"

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Bucket crypto-gold créé")

    today = datetime.now()
    base_path = f"{today.year}/{today.month:02d}/{today.day:02d}"

    tables = {
        "dim_crypto": dim_crypto,
        "dim_date": dim_date,
        "fact_crypto_snapshot": fact
    }

    for name, df in tables.items():

        logger.info(f"Sauvegarde table: {name} | shape: {df.shape}")

        # LOCAL
        local_dir = f"../data/gold/{base_path}"
        os.makedirs(local_dir, exist_ok=True)

        local_file = f"{local_dir}/{name}.parquet"
        df.to_parquet(local_file, index=False, engine="pyarrow")

        logger.info(f"✔ Local saved: {local_file}")

        # MINIO
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        object_name = f"{base_path}/{name}.parquet"

        client.put_object(
            bucket,
            object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )

        logger.info(f"✔ MinIO saved: {object_name}")

    logger.info("Sauvegarde GOLD terminée")

# =====================================
# 4. PIPELINE GOLD
# =====================================

def gold_pipeline(df_silver):

    logger.info("🚀 ===== DÉBUT PIPELINE GOLD =====")

    logger.info(f"Input silver shape: {df_silver.shape}")

    client = get_minio_client()

    dim_crypto = build_dim_crypto(df_silver)
    dim_date = build_dim_date(df_silver)

    fact = build_fact_table(df_silver, dim_crypto, dim_date)

    save_gold(client, dim_crypto, dim_date, fact)

    logger.info("✅ ===== PIPELINE GOLD TERMINÉ =====")

    return dim_crypto, dim_date, fact

# =====================================
# EXECUTION
# =====================================

if __name__ == "__main__":
    print("🚀 Lancement pipeline GOLD")

    # =====================================
    # CHARGEMENT SILVER
    # =====================================
    silver_path = "../data/silver/2026/06/12/crypto.parquet"

    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"Fichier Silver introuvable: {silver_path}")

    df_silver = pd.read_parquet(silver_path)

    print(f"📊 Silver chargé: {df_silver.shape}")

    # =====================================
    # PIPELINE GOLD
    # =====================================
    dim_crypto, dim_date, fact = gold_pipeline(df_silver)

    print("✅ Pipeline terminé avec succès")