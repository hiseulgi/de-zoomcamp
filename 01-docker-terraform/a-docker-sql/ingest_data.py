import os
import argparse

from time import time
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # file name correction from url
    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"

    # download csv file
    if not os.path.exists(csv_name):
        os.system(f"wget {url} -O {csv_name}")
    else:
        print(f"File {csv_name} already exists")

    # postgres connection engine
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    print(f"Connected to PostgreSQL database: {db}")

    # read csv file chunk by chunk
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # create table
    df.head(n=0).to_sql(table_name, engine, if_exists="replace")
    print(f"Table {table_name} created")

    # append first chunk to table
    df.to_sql(table_name, engine, if_exists="append")
    print(f"First chunk appended to {table_name}")

    # process all chunks by simple pipeline
    while True:
        try:
            t_start = time()

            # get next chunk
            df = next(df_iter)

            # convert columns to datetime format
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

            # append data to table in postgresql database from pandas dataframe
            df.to_sql(table_name, engine, if_exists="append")

            t_end = time()

            # logging
            print(f"Chunk processed in {t_end - t_start} seconds")

        except StopIteration:
            print("-" * 50)
            print("! FINISHED !")
            print("All chunks processed")
            print("-" * 50)
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to PostgreSQL")

    parser.add_argument("--user", type=str, help="PostgreSQL user")
    parser.add_argument("--password", type=str, help="PostgreSQL password")
    parser.add_argument("--host", type=str, help="PostgreSQL host")
    parser.add_argument("--port", type=str, help="PostgreSQL port")
    parser.add_argument("--db", type=str, help="PostgreSQL database name")
    parser.add_argument("--table_name", type=str, help="PostgreSQL table name")
    parser.add_argument("--url", type=str, help="Url to CSV file")

    args = parser.parse_args()

    main(args)
