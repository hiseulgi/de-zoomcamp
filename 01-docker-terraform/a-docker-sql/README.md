# Docker Postgres + PGAdmin

## Walkthrough Guide

1. Make `ingest_data.py` program for ingesting CSV data into the Postgres database
2. Build the `ingest_data` Docker image (`Dockerfile`)
3. Run `docker-compose up -d` to start the Postgres and PGAdmin containers
4. Run simple Python HTTP server (`python -m http.server`) to serve the CSV data (`scripts/serve_data.sh`)
5. Run `ingest_data` container to ingest data into the Postgres database (`scripts/ingest_data.sh`)

## Acknowledgements
- [Data Engineering Zoomcamp - Week 1](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform)
- [Data Engineering Zoomcamp - Docker SQL (Week 1)](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/2_docker_sql)
- [NYC TLC CSV Data (Archived)](https://github.com/DataTalksClub/nyc-tlc-data/releases/)