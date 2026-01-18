import pandas as pd
import s3fs
from elasticsearch import Elasticsearch, helpers


def index_to_elastic(execution_date: str):
    """
    execution_date attendu: "YYYY-MM-DD"
    Lit les parquets formatted du jour et les indexe dans Elasticsearch.
    """
    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": "http://minio:9000"},
        key="minioadmin",
        secret="minioadmin"
    )

    es = Elasticsearch("http://elasticsearch:9200")

    year = execution_date[0:4]
    month = execution_date[5:7]
    day = execution_date[8:10]

    formatted_glob = f"datalake/formatted/airsoft/ads/year={year}/month={month}/day={day}/*.parquet"

    try:
        files = fs.glob(formatted_glob)
        if not files:
            print(f"[INDEX] No formatted parquet files found for {execution_date} ({formatted_glob}).")
            return

        dfs = []
        for f in files:
            dfs.append(pd.read_parquet(f"s3://{f}", filesystem=fs))

        df = pd.concat(dfs, ignore_index=True)

        if df.empty:
            print(f"[INDEX] Parquet exists but contains 0 rows for {execution_date}. Nothing to index.")
            return

        df = df.where(pd.notnull(df), None)

        actions = (
            {
                "_index": "airsoft-ads",
                "_source": record
            }
            for record in df.to_dict(orient="records")
        )

        ok, errors = helpers.bulk(es, actions, raise_on_error=False)
        if errors:
            print(f"[INDEX] Indexed {ok} docs with some errors (showing first 3): {errors[:3]}")
        else:
            print(f"[INDEX] Indexed {ok} ads into 'airsoft-ads'.")

    except Exception as e:
        print(f"[INDEX] Indexing error: {e}")


if __name__ == "__main__":
    exec_date = "2026-01-18"
    index_to_elastic(exec_date)
