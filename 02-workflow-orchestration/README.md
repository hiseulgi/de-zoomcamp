# Workflow Orchestration

## Intro to Mage

To install Mage, we can follow this resource: [Mage Zoomcamp - Github Repo](https://github.com/mage-ai/mage-zoomcamp). The Mage will be installed via Docker. Here are the walkthrough steps:
1. Clone the repository
2. Copy the `dev.env` file to `.env` (`cp dev.env .env`)
3. Run the `docker-compose build` command for the first time
4. Run the `docker-compose up -d` command to start the Mage UI
5. Access the Mage UI via `http://localhost:6789`
6. Try to run the example workflow

## ETL: API to Postgres

### Configure Postgres

To configure the Postgres connection in Mage, we need to add some change the `io_config.yaml` file. Here is the example of the configuration:
```yaml
dev:
  POSTGRES_CONNECT_TIMEOUT: 10
  POSTGRES_DBNAME: "{{ env_var('POSTGRES_DBNAME') }}"
  POSTGRES_SCHEMA: "{{ env_var('POSTGRES_SCHEMA') }}"
  POSTGRES_USER: "{{ env_var('POSTGRES_USER') }}"
  POSTGRES_PASSWORD: "{{ env_var('POSTGRES_PASSWORD') }}"
  POSTGRES_HOST: "{{ env_var('POSTGRES_HOST') }}"
  POSTGRES_PORT: "{{ env_var('POSTGRES_PORT') }}"
```

The configuration above is using the environment variables from `.env` file in main directory (`mage-zoomcamp`). Here is the example of the `.env` file:
```env
PROJECT_NAME=magic-zoomcamp
POSTGRES_DBNAME=postgres
POSTGRES_SCHEMA=magic
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```

After that, we can try the connection by creating a new pipeline. The pipeline is `dataloader` then use `SQL` and select `PostgreSQL` and use `dev` profile. Lastly, we can test by run query `SELECT 1` to check the connection.

### ETL Pipeline: API to Postgres

The main goal of this pipeline is to load data from API to Postgres. The API data is from [NYC-Taxi Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz). The pipeline is using the following steps:
1. Load data from API (Extract)
2. Transform the data (Transform)
3. Export the data to Postgres (Load)
4. Check the data in Postgres

### Configure GCP

To configure the GCP connection in Mage, we need to add some change the `io_config.yaml` file. Here are the steps:
1. Create a new GCS bucket (for testing purpose)
2. Create a new service account with `owner` role and download the JSON key
3. Add the JSON key to the main directory (`mage-zoomcamp`)
4. Change the `io_config.yaml` file to use the GCP connection
```yaml
# Google
  GOOGLE_SERVICE_ACC_KEY_FILEPATH: "/home/src/path-to-json-key.json"
  GOOGLE_LOCATION: ASIA-SOUTHEAST2 # Optional
```
5. Test connection by using `BigQuery` data loader block (`SELECT 1;`) and select the profile that we added the GCP connection
6. Another way to test the connection is using `GCS` data loader block
   1. Try upload a file to the bucket
   2. Try change code with:
      ```python
      bucket_name = 'bucket-name'
      object_key = 'uploaded-file.csv'
      ```
   3. Run the code

### ETL Pipeline: API to GCS

The main goal of this pipeline is to load data from API to GCS. We can reuse previous block from the API to Postgres pipeline. In this example we try two different ways to load data to GCS:
1. Upload the single parquet file to GCS
2. Upload by partitioning the parquet file to GCS

Here are the steps to upload the **single parquet** file to GCS:
1. **Load data from API (Extract)**: this block is from the previous pipeline
2. **Transform the data (Transform)**: this block is from the previous pipeline
3. **Export the data to GCS (Load)**: we create new `gcs` data exporter block

And here are the steps to upload by **partitioning the parquet** file to GCS:
1. **Load data from API (Extract)**: this block is from the previous pipeline
2. **Transform the data (Transform)**: this block is from the previous pipeline
3. **Export the data to GCS (Load)**: we create new `Generic (no template)` block and use the following code:
```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/src/path-to-json-key.json"

bucket_name = "bucket_name"
project_id = "project_id"

table_name = "nyc_taxi_data"

root_path = f"{bucket_name}/{table_name}"

@data_exporter
def export_data(data, *args, **kwargs):
    data["tpep_pickup_date"] = data["tpep_pickup_datetime"].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=["tpep_pickup_date"],
        filesystem=gcs
    )
```
4. Check the data in GCS and the data will be partitioned by `tpep_pickup_date` as the folder name

### ETL Pipeline: GCS to BigQuery

The main goal of this pipeline is to load data from GCS to BigQuery. We can reuse previous block from the API to GCS pipeline. Here are the steps:
1. **Load data from GCS (Extract)**: this block is from the previous pipeline
2. **Transform the data (Transform)**: this block is from the previous pipeline
3. **Export the data to BigQuery (Load)**: we create new `bigquery` data exporter block

After that, we can create a new trigger to schedule the pipeline to run every day. To do that, we can create a new trigger in the `Triggers` tab and select the pipeline that we want to schedule. We can set the schedule to run every day at 00:00.

### Parameterized Execution

Parameterized execution is a way to run the pipeline with different parameters. There is `**kwargs` parameter in Mage block that can be used to pass the parameters. Here is the example of the parameterized execution:
```python
@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    # Get the execution date from the kwargs
    now = kwargs.get("execution_date")
    now_fpath = now.strftime("%Y/%m/%d")

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'mage-zoomcamp'
    object_key = f'{now_fpath}/daily-trips.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
```

According to the example above, we can pass the `execution_date` parameter to the block. We can also pass the other parameters to the block, when we create the trigger.

Another thing in the parameterized execution is backfilling. Backfilling is a way to run the pipeline with different parameters for the past dates. **Data backfilling** is the process of retroactively processing historical data or **replacing old records with new ones** as part of an update. 

We can use the `Backfill` tab in the Mage UI to run the pipeline with different parameters for the past dates.

## Acknowledgements
- [Data Engineering Zoomcamp - Week 2](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration)