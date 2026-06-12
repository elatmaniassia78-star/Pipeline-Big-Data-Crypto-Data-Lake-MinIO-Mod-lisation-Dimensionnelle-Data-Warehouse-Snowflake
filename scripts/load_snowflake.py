import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging

# =====================================
# LOGGING
# =====================================

logging.basicConfig(
    filename="snowflake_load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# =====================================
# CONFIG SNOWFLAKE
# =====================================

SNOWFLAKE_CONFIG = {
    "user": "laila",
    "password": "LailaDataAnalyst2026",
    "account": "pj74301.eu-west-3.aws",
    "warehouse": "COMPUTE_WH",
    "database": "CRYPTO_DB",
    "schema": "GOLD"
}

# =====================================
# CONNEXION
# =====================================

def get_connection():

    conn = snowflake.connector.connect(
        user="laila",
        password="LailaDataAnalyst2026",
        account="pj74301.eu-west-3.aws",
        warehouse="COMPUTE_WH"
    )

    return conn

# =====================================
# CREATION TABLES
# =====================================

def create_tables(conn):

    cursor = conn.cursor()

    try:

        cursor.execute("CREATE DATABASE IF NOT EXISTS CRYPTO_DB")
        cursor.execute("USE DATABASE CRYPTO_DB")

        cursor.execute("CREATE SCHEMA IF NOT EXISTS GOLD")
        cursor.execute("USE SCHEMA GOLD")

        # Supprimer les tables existantes
        cursor.execute("DROP TABLE IF EXISTS FACT_CRYPTO_SNAPSHOT")
        cursor.execute("DROP TABLE IF EXISTS DIM_DATE")
        cursor.execute("DROP TABLE IF EXISTS DIM_CRYPTO")

        # DIM_CRYPTO
        cursor.execute("""
        CREATE TABLE DIM_CRYPTO (
            CRYPTO_KEY INTEGER,
            ID STRING,
            SYMBOL STRING,
            NAME STRING
        )
        """)

        # DIM_DATE
        cursor.execute("""
        CREATE TABLE DIM_DATE (
            DATE_KEY INTEGER,
            DATE DATE,
            YEAR INTEGER,
            MONTH INTEGER,
            DAY INTEGER,
            HOUR INTEGER,
            MINUTE INTEGER,
            WEEK INTEGER,
            QUARTER INTEGER
        )
        """)

        # FACT
        cursor.execute("""
        CREATE TABLE FACT_CRYPTO_SNAPSHOT (
            CRYPTO_KEY INTEGER,
            DATE_KEY INTEGER,
            CURRENT_PRICE FLOAT,
            MARKET_CAP FLOAT,
            MARKET_CAP_RANK INTEGER,
            TOTAL_VOLUME FLOAT,
            HIGH_24H FLOAT,
            LOW_24H FLOAT,
            PRICE_CHANGE_24H FLOAT,
            PRICE_CHANGE_PERCENTAGE_24H FLOAT,
            CIRCULATING_SUPPLY FLOAT,
            MAX_SUPPLY FLOAT,
            ATH FLOAT
        )
        """)

        conn.commit()

        print("✅ Tables créées avec succès")

    finally:
        cursor.close()
# =====================================
# LOAD TABLE
# =====================================

def load_table(conn, df, table_name):

    # Colonnes Snowflake en MAJUSCULES
    df.columns = [col.upper() for col in df.columns]

    # Correction DIM_DATE
    if table_name == "DIM_DATE":
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name
    )

    if success:
        print(f"✅ {table_name}: {nrows} lignes chargées")
    else:
        raise Exception(f"Erreur chargement {table_name}")

# =====================================
# VALIDATION
# =====================================

def validate_tables(conn):

    cursor = conn.cursor()

    try:

        tables = [
            "DIM_CRYPTO",
            "DIM_DATE",
            "FACT_CRYPTO_SNAPSHOT"
        ]

        for table in tables:

            cursor.execute(
                f"SELECT COUNT(*) FROM {table}"
            )

            count = cursor.fetchone()[0]

            print(f"{table}: {count} lignes")

            logger.info(
                f"{table}: {count} lignes"
            )

        cursor.execute("""
        SELECT COUNT(*)
        FROM FACT_CRYPTO_SNAPSHOT f
        LEFT JOIN DIM_CRYPTO d
        ON f.CRYPTO_KEY = d.CRYPTO_KEY
        WHERE d.CRYPTO_KEY IS NULL
        """)

        missing_crypto = cursor.fetchone()[0]

        cursor.execute("""
        SELECT COUNT(*)
        FROM FACT_CRYPTO_SNAPSHOT f
        LEFT JOIN DIM_DATE d
        ON f.DATE_KEY = d.DATE_KEY
        WHERE d.DATE_KEY IS NULL
        """)

        missing_date = cursor.fetchone()[0]

        print(
            f"FK crypto manquantes : {missing_crypto}"
        )
        print(
            f"FK date manquantes : {missing_date}"
        )

    finally:
        cursor.close()

# =====================================
# PIPELINE SNOWFLAKE
# =====================================

def snowflake_pipeline():

    logger.info("Début pipeline Snowflake")

    dim_crypto = pd.read_parquet(
        "../data/gold/2026/06/12/dim_crypto.parquet"
    )

    dim_date = pd.read_parquet(
        "../data/gold/2026/06/12/dim_date.parquet"
    )

    fact = pd.read_parquet(
        "../data/gold/2026/06/12/fact_crypto_snapshot.parquet"
    )

    conn = get_connection()

    try:

        create_tables(conn)

        load_table(
            conn,
            dim_crypto,
            "DIM_CRYPTO"
        )

        load_table(
            conn,
            dim_date,
            "DIM_DATE"
        )

        load_table(
            conn,
            fact,
            "FACT_CRYPTO_SNAPSHOT"
        )

        validate_tables(conn)

        logger.info(
            "Pipeline Snowflake terminé"
        )

    finally:
        conn.close()
        print("\n=== DIM_CRYPTO ===")
    print(dim_crypto.head())

    print("\n=== DIM_DATE ===")
    print(dim_date.head())
    print(dim_date.dtypes)

    print("\n=== FACT ===")
    print(fact.head())
    # =====================================
# EXECUTION
# =====================================

if __name__ == "__main__":

    print(
        "🚀 Chargement Snowflake"
    )

    snowflake_pipeline()

    print(
        "✅ Fin du chargement"
    )