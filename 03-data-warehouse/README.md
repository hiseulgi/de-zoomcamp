# Data Warehouse

## OLAP vs OLTP

In Data Sxience, when we're discussing data processing systems, there are 2 main types: **OLAP** and **OLTP** systems.

* ***OLTP***: Online Transaction Processing.
* ***OLAP***: Online Analytical Processing.

An intuitive way of looking at both of these systems is that OLTP systems are "classic databases" whereas OLAP systems are catered for advanced data analytics purposes.

|   | OLTP | OLAP |
|---|---|---|
| Purpose | Control and run essential business operations in real time | Plan, solve problems, support decisions, discover hidden insights |
| Data updates | Short, fast updates initiated by user | Data periodically refreshed with scheduled, long-running batch jobs |
| Database design | Normalized databases for efficiency | Denormalized databases for analysis |
| Space requirements | Generally small if historical data is archived | Generally large due to aggregating large datasets |
| Backup and recovery | Regular backups required to ensure business continuity and meet legal and governance requirements | Lost data can be reloaded from OLTP database as needed in lieu of regular backups |
| Productivity | Increases productivity of end users | Increases productivity of business managers, data analysts and executives |
| Data view | Lists day-to-day business transactions | Multi-dimensional view of enterprise data |
| User examples | Customer-facing personnel, clerks, online shoppers | Knowledge workers such as data analysts, business analysts and executives |

## Data Warehouse

Data warehouse is a system used for reporting and data analysis, and is an OLAP system. It is considered a core component of business intelligence.

A DW receives data from different **data sources** which is then processed in a **staging area** before being ingested to the actual **warehouse** (a database) and arranged as needed. DWs may then feed data to separate **Data Marts**; smaller database systems which end users may use for different purposes.

![Data Warehouse](assets/03_01.jpeg)

## BigQuery

BigQuery (BQ) is a Data Warehouse solution offered by Google Cloud Platform.
* BQ is ***serverless***. There are no servers to manage or database software to install; this is managed by Google and it's transparent to the customers.
* BQ is ***scalable*** and has ***high availability***. Google takes care of the underlying software and infrastructure.
* BQ has built-in features like Machine Learning, Geospatial Analysis and Business Intelligence among others.
* BQ maximizes flexibility by separating data analysis and storage in different _compute engines_, thus allowing the customers to budget accordingly and reduce costs.

Some alternatives to BigQuery from other cloud providers would be AWS Redshift or Azure Synapse Analytics.

## Pricing

BigQuery pricing is divided in 2 main components: processing and storage. There are also additional charges for other operations such as ingestion or extraction. The cost of storage is fixed and at the time of writing is US$0.02 per GB per month; you may check the current storage pricing [in this link](https://cloud.google.com/bigquery/pricing#storage).

Data processing has a [2-tier pricing model](https://cloud.google.com/bigquery/pricing#analysis_pricing_models):
*  On demand pricing (default): US$5 per TB per month; the first TB of the month is free.
*  Flat rate pricing: based on the number of pre-requested _slots_ (virtual CPUs).
   *  A minimum of 100 slots is required for the flat-rate pricing which costs US$2,000 per month.
   *  Queries take up slots. If you're running multiple queries and run out of slots, the additional queries must wait until other queries finish in order to free up the slot. On demand pricing does not have this issue.
   *  The flat-rate pricing only makes sense when processing more than 400TB of data per month.
  
When running queries on BQ, the top-right corner of the window will display an approximation of the size of the data that will be processed by the query. Once the query has run, the actual amount of processed data will appear in the _Query results_ panel in the lower half of the window. This can be useful to quickly calculate the cost of the query.

## External tables

BigQuery supports a few [_external data sources_](https://cloud.google.com/bigquery/external-data-sources): you may query these sources directly from BigQuery even though the data itself isn't stored in BQ.

An ***external table*** is a table that acts like a standard BQ table. The table metadata (such as the schema) is stored in BQ storage but the data itself is external.

You may create an external table from a CSV or Parquet file stored in a Cloud Storage bucket:

```sql
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);
```

This query will create an external table based on 2 CSV files. BQ will figure out the table schema and the datatypes based on the contents of the files.

Be aware that BQ cannot determine processing costs of external tables.

You may import an external table into BQ as a regular internal table by copying the contents of the external table into a new internal table. For example:

```sql
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_non_partitoned AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```

## Partitions

BQ tables can be ***partitioned*** into multiple smaller tables. For example, if we often filter queries based on date, we could partition a table based on date so that we only query a specific sub-table based on the date we're interested in.

[Partition tables](https://cloud.google.com/bigquery/docs/partitioned-tables) are very useful to improve performance and reduce costs, because BQ will not process as much data per query.

You may partition a table by:
* ***Time-unit column***: tables are partitioned based on a `TIMESTAMP`, `DATE`, or `DATETIME` column in the table.
* ***Ingestion time***: tables are partitioned based on the timestamp when BigQuery ingests the data.
* ***Integer range***: tables are partitioned based on an integer column.

For Time-unit and Ingestion time columns, the partition may be daily (the default option), hourly, monthly or yearly.

>Note: BigQuery limits the amount of partitions to 4000 per table. If you need more partitions, consider [clustering](##clustering) as well.

Here's an example query for creating a partitioned table:

```sql
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```

BQ will identify partitioned tables with a specific icon. The _Details_ tab of the table will specify the field which was used for partitioning the table and its datatype.

Querying a partitioned table is identical to querying a non-partitioned table, but the amount of processed data may be drastically different. Here are 2 identical queries to the non-partitioned and partitioned tables we created in the previous queries:

```sql
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
```
* Query to non-partitioned table.
* It will process around 1.6GB of data/

```sql
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
```
* Query to partitioned table.
* It will process around 106MB of data.

You may check the amount of rows of each partition in a partitioned table with a query such as this:

```sql
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;
```

This is useful to check if there are data imbalances and/or biases in your partitions.

## Clustering

***Clustering*** consists of rearranging a table based on the values of its columns so that the table is ordered according to any criteria. Clustering can be done based on one or multiple columns up to 4; the ***order*** of the columns in which the clustering is specified is important in order to determine the column priority.

Clustering may improve performance and lower costs on big datasets for certain types of queries, such as queries that use filter clauses and queries that aggregate data.

>Note: tables with less than 1GB don't show significant improvement with partitioning and clustering; doing so in a small table could even lead to increased cost due to the additional metadata reads and maintenance needed for these features.

Clustering columns must be ***top-level***, ***non-repeated*** columns. The following datatypes are supported:
* `DATE`
* `BOOL`
* `GEOGRAPHY`
* `INT64`
* `NUMERIC`
* `BIGNUMERIC`
* `STRING`
* `TIMESTAMP`
* `DATETIME`

A partitioned table can also be clustered. Here's an example query for creating a partitioned and clustered table:

```sql
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```

Just like for partitioned tables, the _Details_ tab for the table will also display the fields by which the table is clustered.

Here are 2 identical queries, one for a partitioned table and the other for a partitioned and clustered table:

```sql
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;
```
* Query to non-clustered, partitioned table.
* This will process about 1.1GB of data.

```sql
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;
```
* Query to partitioned and clustered data.
* This will process about 865MB of data.

## Partitioning vs Clustering

As mentioned before, you may combine both partitioning and clustering in a table, but there are important differences between both techniques that you need to be aware of in order to decide what to use for your specific scenario:

| Clustering | Partitioning |
|---|---|
| Cost benefit unknown. BQ cannot estimate the reduction in cost before running a query. | Cost known upfront. BQ can estimate the amount of data to be processed before running a query. |
| High granularity. Multiple criteria can be used to sort the table. | Low granularity. Only a single column can be used to partition the table. |
| Clusters are "fixed in place". | Partitions can be added, deleted, modified or even moved between storage options. |
| Benefits from queries that commonly use filters or aggregation against multiple particular columns. | Benefits when you filter or aggregate on a single column. |
| Unlimited amount of clusters; useful when the cardinality of the number of values in a column or group of columns is large. | Limited to 4000 partitions; cannot be used in columns with larger cardinality. |

You may choose clustering over partitioning when partitioning results in a small amount of data per partition, when partitioning would result in over 4000 partitions or if your mutation operations modify the majority of partitions in the table frequently (for example, writing to the table every few minutes and writing to most of the partitions each time rather than just a handful).

BigQuery has _automatic reclustering_: when new data is written to a table, it can be written to blocks that contain key ranges that overlap with the key ranges in previously written blocks, which weaken the sort property of the table. BQ will perform automatic reclustering in the background to restore the sort properties of the table.
* For partitioned tables, clustering is maintaned for data within the scope of each partition.