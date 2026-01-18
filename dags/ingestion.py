import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today("UTC").add(days=-1),
}

def run_ingestion(**kwargs):
    import scraper
    scraper_instance = scraper.Scraper()
    sitemap_url = "https://www.airsoft-occasion.fr/sitemap?page=1"
    urls = scraper_instance.get_urls_from_sitemap(sitemap_url)
    if not urls:
        return

    scraped_data = []
    for url in urls:
        data = scraper_instance.scrape_ad(url)
        if data:
            scraped_data.append(data)

    scraper_instance.save_to_datalake(scraped_data, kwargs.get("logical_date"))

def run_indexer(**kwargs):
    import sys
    sys.path.append("/opt/airflow/scripts")
    import indexer
    execution_date = kwargs.get("logical_date").strftime("%Y-%m-%d")
    indexer.index_to_elastic(execution_date)

with DAG(
    "1_ingestion_complete_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="scrape_raw",
        python_callable=run_ingestion,
    )

    spark_conf = {
        "spark.pyspark.driver.python": "python3.12",
        "spark.pyspark.python": "python3.12",

        "spark.master": "spark://spark-master:7077",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "minioadmin",
        "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
    }

    clean_task = SparkSubmitOperator(
        task_id="spark_clean_formatted",
        conn_id="spark_default",
        application="/opt/airflow/scripts/spark_cleaning.py",
        application_args=["{{ logical_date.strftime('year=%Y/month=%m/day=%d') }}"],
        conf=spark_conf,
        verbose=True,
    )

    agg_task = SparkSubmitOperator(
        task_id="spark_aggregate_usage",
        conn_id="spark_default",
        application="/opt/airflow/scripts/spark_aggregation.py",
        application_args=["{{ logical_date.strftime('year=%Y/month=%m/day=%d') }}"],
        conf=spark_conf,
        verbose=True,
    )

    index_task = PythonOperator(
        task_id="index_to_elastic",
        python_callable=run_indexer,
    )

    ingest_task >> clean_task >> agg_task >> index_task
