#! /bin/bash 

# URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
URL="http://172.17.0.1:8000/data/yellow_tripdata_2021-01.csv"

docker run -it \
    --network=pg-network \
    taxi_ingest:0.0.1 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
