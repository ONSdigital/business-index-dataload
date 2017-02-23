# BI Dataload step 1: convert raw data to Parquet files #


![MacDown Screenshot](./BI-data-ingestion-Spark-flow-step-1.jpg)

## Why? ##

* Initially, the application will consume data in different text-based formats.
* PAYE and VAT data will be provided via CSV extracts from the IDBR.
* Companies House data will be provided from the monthly CSV extract from the Companies House website.
* Links between CH and PAYE/VAT data are provided as a JSON file which is generated from a separate machine learning application.
* We will be processing the data with Apache Spark on Cloudera.
* Spark can process Parquet files much more flexibly and efficiently than raw CSV or JSON data.
* Also, the incoming data formats may change.
* We convert the incoming files to Parquet as a first step, so that we can perform subsequent processing steps more efficiently.

## How? ##
