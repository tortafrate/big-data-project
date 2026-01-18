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

    t = text.replace("\xa0", " ").strip()

    m = re.search(r"(\d{1,3}(?:[ .]\d{3})*(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?)\s*(€|EUR)\b", t, re.IGNORECASE)
    if not m:
        return None

    num = m.group(1)
    num = num.replace(" ", "")
    if "." in num and "," not in num:
        parts = num.split(".")
        if all(len(p) == 3 for p in parts[1:]):
            num = "".join(parts)
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

    extract_price_udf = udf(extract_price, DoubleType())
    clean_title_udf = udf(clean_title, StringType())

    out_schema = StructType([
        StructField("url", StringType(), True),
        StructField("scraped_at", StringType(), True),   # conservé tel quel (iso string)
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("source", StringType(), True),
        StructField("processing_time", TimestampType(), True),
    ])

    try:
        df_raw = (
            spark.read
            .option("multiLine", "true")
            .json(raw_path)
        )

        if df_raw.rdd.isEmpty():
            print("[CLEAN] No RAW rows found. Writing empty FORMATTED with schema.")
            empty_df = spark.createDataFrame([], schema=out_schema)
            empty_df.write.mode("overwrite").parquet(formatted_output)
            return

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
            .filter(col("url").isNotNull())
        )

        if df_clean.rdd.isEmpty():
            print("[CLEAN] No rows after cleaning. Writing empty FORMATTED with schema.")
            empty_df = spark.createDataFrame([], schema=out_schema)
            empty_df.write.mode("overwrite").parquet(formatted_output)
            return

        df_clean.write.mode("overwrite").parquet(formatted_output)
        print(f"[CLEAN] Cleaning completed. Rows written: {df_clean.count()}")

    except Exception as e:
        print(f"[CLEAN] Error during cleaning: {e}")
        print("[CLEAN] Writing empty FORMATTED with schema to keep pipeline consistent.")
        empty_df = spark.createDataFrame([], schema=out_schema)
        empty_df.write.mode("overwrite").parquet(formatted_output)


if __name__ == "__main__":
    exec_partition = sys.argv[1] if len(sys.argv) > 1 else "year=2026/month=01/day=18"
    run_cleaning(exec_partition)