# BI Dataload step 1: convert raw data to Parquet files #


![MacDown Screenshot](./BI-data-ingestion-Spark-flow-step-1.jpg)

* [README](../README.md)

> * [Step 1](./bi-dataload-step-1.md).
> * [Step 2](./bi-dataload-step-2.md).
> * [Step 3](./bi-dataload-step-3.md).

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

### Data file locations ###

* All files are held in HDFS.
* The locations are specified via various configuration properties.
* These have default values in the `src/main/resources/application.conf` file.
* They can be modified via environment variables - see the configuration file for details.
* We can also provide values at runtime here using "-D" Java options in the Oozie task specification.
* For example, to set the source data directory:

> * `--driver-java-options "-Ddataload.src-data.dir=./bi-data"`

* The default directory structure is:

> *  `bi-data`

>> * `CH`: Companies House CSV file(s) - CH download is multiple files.
>> * `LINKS`: Links JSON file
>> * `PAYE`: PAYE CSV file
>> * `VAT`: VAT CSV file
>> * `WORKINGDATA`:  All generated Parquet files.


### Data formats ###

* Links data is provided in a nested JSON format like this:

`{"UBRN":"12345678","CH":["COMPANYNO"],"VAT":["VATREFERENCE"],"PAYE":["PAYEREFERENCE"]}`
  
* The UBRN is currently generated as part of the Linking process (this may change).
* In theory, there could be several VAT or PAYE entries for a given UBRN.
* We expect there only to be ONE Company for each UBRN i.e. there is no real need for the "CH" field to be a list here.
* VAT and PAYE data is currently provided in a CSV format generated from the IDBR.
* Companies House data is provided in a [publicly available CSV format](http://resources.companieshouse.gov.uk/toolsToHelp/pdf/freeDataProductDataset.pdf).
* We copy ALL fields from the source data and store them in the corresponding Parquet files.
* Parquet files hodl their data schema, and store data in a compressed columnar format.
* This means subsequent processing steps can select specific columns much more efficiently without having to load the entire data-set e.g. for joining the Links to CH/PAYE/VAT data.

#### Column headings ####

* We want to use Spark SQL and data-frames wherever possible for processing this data.
* This is easier if our column names meet SQL standards for column identifiers.
* Several of the CSV columns (e.g. for Companies House) include dots and spaces which cause problems in SQL.
* We therefore remove dots and spaces from the column headings during the CSV upload process.
* For example CH column `SICCode.SicText_1` is re-named as `SICCodeSicText_1`.
* You may need to check these conversions if the CSV file formats change.
 
### Spark CSV ###

* This step uses the popular Spark CSV package for parsing and loading data from CSV files directly into Spark's DataFrame format.
* In Spark 2.x, this package is part of the standard Spark installation, so there is no need to install it separately.
* However, our Cloudera cluster is currently on Apache Spark 1.6.0, so we need to load the Spark CSV package and its dependencies separately.
* The corresponding JAR files must be specified at runtime.

> * `spark-csv_2.10-1.5.0.jar`: Spark CSV package
> * `commons-csv-1.1.jar`: Additional dependency for Spark CSV
> * `univocity-parsers-1.5.1.jar`: Additional dependency for Spark CSV

* The JARS should be stored in HDFS so that they can be loaded by the Oozie job at runtime.
* If Spark is upgraded to version 2.x on Cloudera, these libraries will no longer be required.

### Oozie task specification ###

* We use Oozie to execute the Spark processing on Cloudera.
* Each step is defined as a separate task in the work-flow.
* Step 1 should be defined as indicated below.

#### Oozie Task Definition ####

* Assumes files are installed in HDFS `hdfs://dev4/user/appUser`.
* Current implementation completes Step 1 in around 7 minutes on the Dev4 cluster.
* Most of this time is taken for loading the Links file, because processing JSON demands more resources than loading the CSV data.
* This example specifies 8 Spark executors to ensure sufficient resources when loading the JSON file.
* It may be possible to tweak the various Spark memory settings to use less memory, but this configuration seems to work OK with current data-sets.

Page 1 Field | Contents
------------- | -------------
Spark Master  | yarn-cluster
Mode  | cluster
App Name | ONS BI Dataload Step 1 Load Source Data To Parquet
Jars/py files | hdfs://dev4/user/appUser/libs/business-index-dataload_2.10-1.0.jar
Main class | uk.gov.ons.bi.dataload.SourceDataToParquetApp

Page 2 Field | Contents
------------- | -------------
Properties / Options list | --num-executors 8 --driver-memory 2G --executor-memory 3G --jars hdfs://dev4/user/appUser/libs/spark-csv_2.10-1.5.0.jar,hdfs://dev4/user/appUser/libs/univocity-parsers-1.5.1.jar,hdfs://dev4/user/appUser/libs/commons-csv-1.1.jar --driver-java-options "-Xms1g -Xmx5g"

