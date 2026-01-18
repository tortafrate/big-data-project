import sys
import re
from bs4 import BeautifulSoup

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, lit, current_timestamp, coalesce
)
from pyspark.sql.types import (
    StringType, DoubleType, StructType, StructField, TimestampType
)


def _extract_price_from_text(text: str):
    """
    Extrait un prix depuis du texte.
    Supporte : "1200 €", "1 200 €", "1200,50€", "1200.50 EUR", etc.
    Retourne float ou None.
    """
    if not text:
        return None

    # Normalisation
    t = text.replace("\xa0", " ").strip()

    # Cherche un motif prix avant symbole €
    # Ex: 1 200,50 € / 1200 € / 1200.50 EUR
    m = re.search(r"(\d{1,3}(?:[ .]\d{3})*(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?)\s*(€|EUR)\b", t, re.IGNORECASE)
    if not m:
        return None

    num = m.group(1)
    # Retire séparateurs de milliers " " ou "."
    num = num.replace(" ", "")
    # Si on a "1.200" et pas de décimales, c'est probablement un séparateur de milliers.
    # On gère simplement en retirant les points si on a aussi plus de 3 chiffres.
    if "." in num and "," not in num:
        parts = num.split(".")
        # Si tous les blocs sauf le premier font 3 chiffres, on considère milliers
        if all(len(p) == 3 for p in parts[1:]):
            num = "".join(parts)
    # Décimales : remplace , par .
    num = num.replace(",", ".")

    try:
        return float(num)
    except Exception:
        return None


def extract_price(html_content: str):
    """
    Extraction depuis HTML : on prend le texte visible et on cherche un prix.
    """
    if not html_content:
        return None
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(" ", strip=True)
        return _extract_price_from_text(text)
    except Exception:
        return None


def clean_title(html_content: str):
    """
    Titre depuis HTML en fallback (si df_raw.title est manquant).
    """
    if not html_content:
        return None
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        return None
    except Exception:
        return None


def run_cleaning(date_partition: str):
    """
    date_partition attendu: "year=YYYY/month=MM/day=DD"
    """
    spark = (
        SparkSession.builder
        .appName("Airsoft Cleaning")
        .getOrCreate()
    )

    raw_path = f"s3a://datalake/raw/airsoft/ads/{date_partition}/*.json"
    formatted_output = f"s3a://datalake/formatted/airsoft/ads/{date_partition}/"

    print(f"[CLEAN] Reading RAW from: {raw_path}")
    print(f"[CLEAN] Writing FORMATTED to: {formatted_output}")

    # UDFs
    extract_price_udf = udf(extract_price, DoubleType())
    clean_title_udf = udf(clean_title, StringType())

    # Schéma de sortie garanti (pour écrire même si DF vide)
    out_schema = StructType([
        StructField("url", StringType(), True),
        StructField("scraped_at", StringType(), True),   # conservé tel quel (iso string)
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("processing_time", TimestampType(), True),
    ])

    try:
        # IMPORTANT: ton RAW est un JSON "tableau" (json.dumps(list[dict]))
        # Spark doit lire en multiLine, sinon lecture partielle/corrompue.
        df_raw = (
            spark.read
            .option("multiLine", "true")
            .json(raw_path)
        )

        # Si df_raw est vide/invalide, on écrit quand même un parquet vide avec schéma
        if df_raw.rdd.isEmpty():
            print("[CLEAN] No RAW rows found. Writing empty FORMATTED with schema.")
            empty_df = spark.createDataFrame([], schema=out_schema)
            empty_df.write.mode("overwrite").parquet(formatted_output)
            return

        # Normalisation / cleaning
        df_clean = (
            df_raw.select(
                col("url").cast("string").alias("url"),
                col("scraped_at").cast("string").alias("scraped_at"),
                coalesce(
                    col("title").cast("string"),
                    clean_title_udf(col("html_content"))
                ).alias("title"),
                extract_price_udf(col("html_content")).alias("price"),
                coalesce(col("source").cast("string"), lit("airsoft-occasion")).alias("source"),
                current_timestamp().alias("processing_time")
            )
            # On garde les lignes même si price est null, mais on impose au moins une URL
            .filter(col("url").isNotNull())
        )

        # Même si 0 ligne (peu probable après filter url), on écrit un parquet avec schéma
        if df_clean.rdd.isEmpty():
            print("[CLEAN] No rows after cleaning. Writing empty FORMATTED with schema.")
            empty_df = spark.createDataFrame([], schema=out_schema)
            empty_df.write.mode("overwrite").parquet(formatted_output)
            return

        # Write parquet utilisable
        df_clean.write.mode("overwrite").parquet(formatted_output)
        print(f"[CLEAN] Cleaning completed. Rows written: {df_clean.count()}")

    except Exception as e:
        # En cas d'erreur, on écrit quand même un parquet vide schématisé pour ne pas casser la suite
        print(f"[CLEAN] Error during cleaning: {e}")
        print("[CLEAN] Writing empty FORMATTED with schema to keep pipeline consistent.")
        empty_df = spark.createDataFrame([], schema=out_schema)
        empty_df.write.mode("overwrite").parquet(formatted_output)


if __name__ == "__main__":
    # Airflow passe "year=YYYY/month=MM/day=DD"
    exec_partition = sys.argv[1] if len(sys.argv) > 1 else "year=2026/month=01/day=18"
    run_cleaning(exec_partition)