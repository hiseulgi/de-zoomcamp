## Intro

- **Data ingestion is the process of extracting data** from a producer, transporting it to a convenient environment, and preparing it for usage by normalising it, sometimes cleaning, and adding metadata.
- In many data science teams, **data magically appears** - because the engineer loads it.
- Building pipelines:
  - Extracting data
  - Normalising, cleaning, adding metadata such as schema and types
  - and Incremental loading, which is vital for fast, cost effective data refreshes.
- Data engineer’s **main goal is to ensure data flows** from source systems to analytical destinations
- Besides, **data engineers are responsible for building and maintaining the infrastructure** that enables data generation, storage, and usage.

## Data Extraction

As a data engineer, you will be responsible for extracting data from various sources, such as databases, APIs, and logs. This data can be structured or unstructured, and it can be stored in a variety of formats, such as CSV, JSON, or Parquet.

Besides, to prevent the extraction pipeline from breaking, we need to consider the following:
- **Hardware limits**: Navigate the challenges of managing memory.
- **Network limits**: Sometimes networks can fail. We can’t fix what could go wrong but we can retry network jobs until they succeed.
- **Source API limits**: Each API has its own rate limits. We need to be aware of these limits and design our extraction pipeline accordingly.

### Hardware Limits

- We often don't know the size of the data we are extracting and can't scale our memory to fit the data. So, we can **control the max memory usage**.
- Data engineers primarily focus on **streaming data between buffers**, such as from APIs to local files, from webhooks to event queues, and from event queues (e.g., Kafka, SQS) to storage buckets.
- In Python, data engineers **commonly use generators to process data in streams**. Generators are functions that can yield multiple returns, enabling data to be released as it's produced, rather than returning it all at once as a batch.
- For instance, when retrieving data from a source like Twitter, where the volume of data is uncertain and could potentially exceed memory capacity, using a generator ensures efficient handling of the data without memory issues.
- **Generators allow functions to return multiple times**, enabling a continuous flow of data. This contrasts with regular functions, which return all data at once.

**Regular function to extract data:**
```python
def search_twitter(query):
    data = []
    for row in paginated_get(query):
        data.append(row)
    return data

# Collect all the cat picture data
for row in search_twitter("cat pictures"):
    # Once collected, 
    # print row by row
    print(row)
```
When calling `for row in search_twitter("cat pictures"):` all the data must first be downloaded before the first record is returned. This can be problematic if the data is large and exceeds memory capacity.

**Generator function to extract data:**
```python
def search_twitter(query):
    for row in paginated_get(query):
        yield row

# Collect all the cat picture data
for row in search_twitter("cat pictures"):
    # Once collected, 
    # print row by row
    print(row)
```
When calling `for row in search_twitter("cat pictures"):` the data is returned row by row, as it is downloaded. This is more memory efficient.

### Extraction Example

#### Example 1: Grabbing Data from an API

- **Pros**: Easy memory management thanks to api returning events/pages
- **Cons**: Low throughput, due to the data transfer being constrained via an API.

```python
import requests

BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

# I call this a paginated getter
# as it's a function that gets data
# and also paginates until there is no more data
# by yielding pages, we "microbatch", which speeds up downstream processing

def paginated_getter():
    page_number = 1

    while True:
        # Set the query parameters
        params = {'page': page_number}

        # Make the GET request to the API
        response = requests.get(BASE_API_URL, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        page_json = response.json()
        print(f'got page number {page_number} with {len(page_json)} records')

        # if the page has no records, stop iterating
        if page_json:
            yield page_json
            page_number += 1
        else:
            # No more data, break the loop
            break

if __name__ == '__main__':
    # Use the generator to iterate over pages
    for page_data in paginated_getter():
        # Process each page as needed
        print(page_data)
```

#### Example 2: Grabbing Data from a File - Simple Download

- **Pros**: High throughput, as the data transfer is not constrained by an API.
- **Cons**: Harder memory management, as the file could be large.

```python
import requests
import json

url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"

def download_and_read_jsonl(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    data = response.text.splitlines()
    parsed_data = [json.loads(line) for line in data]
    return parsed_data
   

downloaded_data = download_and_read_jsonl(url)

if downloaded_data:
    # Process or print the downloaded data as needed
    print(downloaded_data[:5])  # Print the first 5 entries as an example
```

#### Example 3: Grabbing Data from a File - Streaming Download (Generator)

- **Pros**: High throughput, easy memory management, because we are downloading a file
- **Cons**: Difficult to do for columnar file formats, as entire blocks need to be downloaded before they can be deserialised to rows. Sometimes, the code is complex too.

```python
import requests
import json

def download_and_yield_rows(url):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an HTTPError for bad responses

    for line in response.iter_lines():
        if line:
            yield json.loads(line)

# Replace the URL with your actual URL
url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"

# Use the generator to iterate over rows with minimal memory usage
for row in download_and_yield_rows(url):
    # Process each row as needed
    print(row)

# Nb: the result was faster than the previous example (Example 2)
```

## Data Normalisation

- Data cleaning often involves two main aspects: **normalizing data without altering its meaning** and **filtering data to suit a specific use case**.
- Normalizing data includes tasks such as adding data types (e.g., converting strings to numbers or timestamps), renaming columns to adhere to downstream standards, flattening nested dictionaries, and unnesting lists or arrays into separate tables.
- Preparing data is necessary because JSON lacks a schema, leading to ambiguity about its contents. Without enforced types, inconsistencies like variations in how data is represented can occur, potentially causing downstream issues.
- Using JSON data directly is not ideal due to difficulties in understanding its structure, lack of enforced types leading to inconsistencies, and inefficiencies in memory usage and data processing compared to columnar formats like Parquet or databases.
- JSON is **primarily designed for interchange and data transfer**, not for direct analytical usage, as it is slower for aggregation and search compared to columnar formats.

### Intro to DLT

- dlt is a Python library designed to **simplify, accelerate, and enhance the robustness** of data pipelines for data engineers.
- It functions as a loading tool, incorporating industry best practices into pipelines in a declarative manner, minimizing the need for manual implementation.
- By leveraging dlt, data engineers can expedite pipeline development and avoid repetitive tasks, increasing efficiency.
- **Key features** of dlt include automated schema inference and evolution, data typing, structure flattening, column renaming to align with database standards, memory-efficient processing of event streams, and versatile loading capabilities to various databases or file formats.

## Incremental Loading

- Incremental loading involves **updating datasets with new data without reloading the entire dataset**, resulting in faster and more cost-effective pipeline execution.
- It is closely related to **incremental extraction and state management**, which track what data has been loaded and what remains to be loaded.
- dlt **stores state information** at the destination in a separate table to facilitate incremental loading.
- Incremental extraction involves requesting only the new or changed data, tightly connected to state management.
- dlt supports two methods of incremental loading: 
  - **Append**, suitable for immutable or stateless events 
  - **Merge**, used for updating changing data.
- Examples:
  - **Append** is useful for scenarios like adding new taxi ride records daily or creating slowly changing dimension tables for auditing changes.
  - **Merge** is employed for updating data that changes, such as updating the payment status of taxi rides from "booked" to "paid" or "rejected."