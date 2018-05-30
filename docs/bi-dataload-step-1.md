# BI Dataload step 1: convert business data to Parquet files #


![MacDown Screenshot](./bi-dataload-step-1-data-flow.jpg)

## Why? ##

* Initially, the application will consume data in different text-based formats.
* PAYE and VAT data will be provided via CSV extracts from the IDBR.
* Companies House data will be provided from the monthly CSV extract from the Companies House website.
* We will be processing the data with Apache Spark on Cloudera.
* Spark can process Parquet files much more flexibly and efficiently than raw CSV or JSON data.
* Also, the incoming data formats may change.
* We convert the incoming files to Parquet as a first step, so that we can perform subsequent processing steps more efficiently.
* Any lookup data, e.g. PAYE TCN (STC) code --> SIC code conversion, will be provided as CSV files and loaded in the same way here.

### Links data pre-processed separately ###

* Links between CH and PAYE/VAT data are provided as a JSON file which is generated from a separate machine learning application.
* However, Links require extra pre-processing which is performed in [step 0](./bi-dataload-step-0.md).
* We do not process Links data in step 1.


## How? ##

### Data file locations ###

* All files are held in HDFS.
* The locations are specified via various configuration properties.
* These have default values in the `src/main/resources/application.conf` file.
* The configuration properties can be modified at runtime via Java driver properties.
* See the [file locations document](./bi-dataload-file-locations.md) for details.

### Data formats ###

* VAT and PAYE data is currently provided in a CSV format generated from the IDBR.
* Companies House data is provided in a [publicly available CSV format](http://resources.companieshouse.gov.uk/toolsToHelp/pdf/freeDataProductDataset.pdf).
* We copy ALL fields from the source data and store them in the corresponding Parquet files.
* Parquet files hold their data schema, and store data in a compressed columnar format.
* This means subsequent processing steps can select specific columns much more efficiently without having to load the entire data-set e.g. for joining the Links to CH/PAYE/VAT data in [step 2](./bi-dataload-step-2.md).


#### Column headings ####

* We want to use Spark SQL and data-frames wherever possible for processing this data.
* This is easier if our column names meet SQL standards for column identifiers.
* Several of the CSV columns (e.g. for Companies House) include dots and spaces which cause problems in SQL.
* We therefore remove dots and spaces from the column headings during the CSV upload process.
* For example CH column `SICCode.SicText_1` is re-named as `SICCodeSicText_1`.
* You may need to check these conversions if the CSV file formats change.
 
### Spark CSV ###

* This process uses the Spark CSV package for writing data directly from a Spark data frame to CSV.
* In Spark 2.x, this package is part of the standard Spark installation, so there is no need to install it separately.

### Oozie task specification ###

* We use Oozie to execute the Spark processing on Cloudera.
* Each step is defined as a separate task in the work-flow.
* Step 1 should be defined as indicated below.

#### Oozie Task Definition ####

* Assumes JAR files are installed in HDFS `hdfs://prod1/user/bi-dev-ci/businessIndex/lib`.
* It may be possible to tweak the various Spark memory settings to use less memory, but this configuration seems to work OK with current data-sets.
* We set the "env" parameter below so the Spark process knows where to read/write application data:

>	`-Dbi-dataload.app-data.env=dev`

* The default value in the config file is "dev", but the parameter is included here to  remind you that you may need to change it.
* The task parameters below also assume we are working in "dev" here.


Page 1 Field | Contents
------------- | -------------
Spark Master  | yarn-cluster
Mode  | cluster
App Name | ONS BI Dataload Step 1 Load Source Data To Parquet
Jars/py files | hdfs://prod1/user/bi-dev-ci/businessIndex/lib/business-index-dataload_2.11-1.5.jar
Main class | uk.gov.ons.bi.dataload.SourceDataToParquetApp

Page 2 Field | Contents
------------- | -------------
Properties / Options list | --num-executors 8 --driver-memory 2G --executor-memory 3G --jars hdfs://prod1/user/bi-dev-ci/businessIndex/lib/config-1.3.2.jar --driver-java-options "-Dbi-dataload.app-data.env=dev -Xms1g -Xmx5g"

* Since the Oozie doesn't support Spark 2.x we now have to use the Oozie shell node and supply a shell script with the spark2-submit command for this process.
* The shell scripts are stored in HDFS `hdfs://prod1/user/bi-dev-ci/businessIndex/lib`.
* If the Oozie version is ever updated we may be able to switch back to using the Spark Job node (or if Oozie shareLib ever replaces spark 1.6 with spark 2.x).

## Further information ##

* [README](../README.md)

> * [File locations](./bi-dataload-file-locations.md).
> * [Step 0](./bi-dataload-step-0.md).
> * [Step 1](./bi-dataload-step-1.md).
> * [Step 2](./bi-dataload-step-2.md).
> * [Step 3](./bi-dataload-step-3.md).
> * [Testing](./bi-dataload-testing.md).
> * [CSV extract](./bi-dataload-csv-extract.md).

