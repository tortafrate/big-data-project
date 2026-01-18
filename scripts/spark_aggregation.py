import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, max, min, current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, TimestampType


def run_aggregation(date_partition: str):
    """
    date_partition attendu: "year=YYYY/month=MM/day=DD"
    """
    spark = (
        SparkSession.builder
        .appName("Airsoft Aggregation")
        .getOrCreate()
    )

    input_path = f"s3a://datalake/formatted/airsoft/ads/{date_partition}/"
    output_path = f"s3a://datalake/usage/airsoft_stats/{date_partition}/"

    print(f"[AGG] Reading FORMATTED from: {input_path}")
    print(f"[AGG] Writing USAGE stats to: {output_path}")

    # Schéma stats garanti
    stats_schema = StructType([
        StructField("avg_price", DoubleType(), True),
        StructField("min_price", DoubleType(), True),
        StructField("max_price", DoubleType(), True),
        StructField("total_ads", LongType(), False),
        StructField("calculation_date", TimestampType(), False),
    ])

    try:
        df = spark.read.parquet(input_path)

        # Si aucune ligne (ou parquet vide), on écrit des stats "zéro"
        if df.rdd.isEmpty():
            print("[AGG] FORMATTED is empty. Writing zero-stats parquet.")
            df_stats = spark.createDataFrame(
                [(None, None, None, 0,)],
                schema=StructType([
                    StructField("avg_price", DoubleType(), True),
                    StructField("min_price", DoubleType(), True),
                    StructField("max_price", DoubleType(), True),
                    StructField("total_ads", LongType(), False),
                ])
            ).withColumn("calculation_date", current_timestamp())

            df_stats.write.mode("overwrite").parquet(output_path)
            return

        # On agrège uniquement sur price non null pour les métriques,
        # mais total_ads compte toutes les annonces.
        total_ads = df.select(count(lit(1)).alias("total_ads"))

        df_price = df.filter(col("price").isNotNull())

        if df_price.rdd.isEmpty():
            print("[AGG] No non-null prices. Writing stats with total_ads only.")
            df_stats = total_ads.select(
                lit(None).cast("double").alias("avg_price"),
                lit(None).cast("double").alias("min_price"),
                lit(None).cast("double").alias("max_price"),
                col("total_ads").cast("long").alias("total_ads"),
            ).withColumn("calculation_date", current_timestamp())
        else:
            df_metrics = df_price.agg(
                avg("price").alias("avg_price"),
                min("price").alias("min_price"),
                max("price").alias("max_price"),
            )
            df_stats = df_metrics.crossJoin(total_ads).withColumn("calculation_date", current_timestamp())

        df_stats.write.mode("overwrite").parquet(output_path)
        print("[AGG] Aggregation completed.")

    except Exception as e:
        # Si erreur (ex: dossier parquet vide), on écrit un parquet de stats "zéro" pour garder un pipeline stable
        print(f"[AGG] Error during aggregation: {e}")
        print("[AGG] Writing zero-stats parquet to keep pipeline consistent.")

        df_stats = spark.createDataFrame(
            [(None, None, None, 0,)],
            schema=StructType([
                StructField("avg_price", DoubleType(), True),
                StructField("min_price", DoubleType(), True),
                StructField("max_price", DoubleType(), True),
                StructField("total_ads", LongType(), False),
            ])
        ).withColumn("calculation_date", current_timestamp())

        df_stats.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    exec_partition = sys.argv[1] if len(sys.argv) > 1 else "year=2026/month=01/day=18"
    run_aggregation(exec_partition)
