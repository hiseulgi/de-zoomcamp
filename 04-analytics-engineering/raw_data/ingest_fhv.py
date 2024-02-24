import os

import pandas as pd
import requests


def download_and_save_file(url, filename):
    """Downloads a file from the given URL and saves it locally."""
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(filename, "wb") as f:
        for chunk in response.iter_content(1024):
            f.write(chunk)

    print(f"Downloaded {filename}")


def load_data():
    """Loads data from local files, combining them into a single DataFrame."""
    filenames = [f"fhv_tripdata_2019-{month:02d}.csv.gz" for month in range(1, 13)]
    return pd.concat(
        pd.read_csv(filename, sep=",", compression="gzip") for filename in filenames
    )


def main():
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"

    # Download files first
    for month in range(2, 13):
        month_str = str(month).zfill(2)
        filename = f"fhv_tripdata_2019-{month_str}.csv.gz"
        url = base_url + filename
        download_and_save_file(url, filename)

    # Load and combine data
    df = load_data()

    # Save combined DataFrame
    df.to_csv("fhv_tripdata_2019.csv", index=False)
    print(df.info())
    print(df.head())


if __name__ == "__main__":
    main()
