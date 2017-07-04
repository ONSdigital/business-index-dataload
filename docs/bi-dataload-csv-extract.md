# BI Dataload: CSV data extract #


## What and why? ##

* In June 2017, HMRC requested a CSV extract of the full Business Index.
* We provided this in the form of 3 CSV files.  
* The files were generated via an extra Scala Spark app in the main BI data ingestion package.

## How? ##

### Data file locations ###

* All data files are held in HDFS.
* The locations are specified via various configuration properties.
* See [file locations](./bi-dataload-step-1.md) for further information.
* Source file (in standard working directory):

> * `BI_Output.parquet` i.e. the output from the main BI data ingestion process.

* CSV outputs (in a directory `EXTRACT` under the working directory):

> * `bi-legal-entities.csv`: contained all BI fields except PAYE and VAT.
> * `bi-paye.csv`: contained ID (UBRN) and PAYE references.
> * `bi-vat.csv`: contained ID (UBRN) and VAT references.

### Data outputs ###

* All the output files were generated via Spark and written to HDFS.
* Thsi means the actual data is written as multiple `part-NNNNN` files inside a directory such as `bi-paye-jun-2017.csv`.
* The Spark process repartitioned the data onto a single partition before output  in order to creat a single `part-NNNNN` file in the relevant output directory.
* This file was then downloaded manually and re-named before being sent to HRMC.

### Spark CSV ###

* This process uses the Spark CSV package for writing data directly from a Spark data frame to CSV.
* In Spark 2.x, this package is part of the standard Spark installation, so there is no need to install it separately.
* However, our Cloudera cluster is currently on Apache Spark 1.6.0, so we need to load the Spark CSV package and its dependencies separately.
* The corresponding JAR files must be specified at runtime.

> * `spark-csv_2.10-1.5.0.jar`: Spark CSV package
> * `commons-csv-1.1.jar`: Additional dependency for Spark CSV
> * `univocity-parsers-1.5.1.jar`: Additional dependency for Spark CSV

* The JARS should be stored in HDFS so that they can be loaded by the Oozie job at runtime.
* If Spark is upgraded to version 2.x on Cloudera, these libraries will no longer be required.


#### Oozie Task Definition ####

* An example Oozie task definition for this process is shown below.
* It may be possible to tweak the various Spark memory settings to use less memory, but this configuration seems to work OK with current data-sets.
* We set the "env" parameter below so the Spark process knows where to read/write application data:

>	`-Dbi-dataload.app-data.env=dev`

* The default value in the config file is "dev", but the parameter is included here to  remind you that you may need to change it.
* The task parameters below also assume we are working in "dev" here.


Page 1 Field | Contents
------------- | -------------
Spark Master  | yarn-cluster
Mode  | cluster
App Name | ONS BI Dataload: Extract BI data to CSV files
Jars/py files | hdfs://dev4/ons.gov/businessIndex/dev/lib/business-index-dataload_2.10-1.4.jar
Main class | uk.gov.ons.bi.dataload.HmrcBiExportApp

Page 2 Field | Contents
------------- | -------------
Properties / Options list | --num-executors 8 --driver-memory 2G --executor-memory 3G --jars hdfs://dev4/ons.gov/businessIndex/dev/lib/spark-csv_2.10-1.5.0.jar,hdfs://dev4/ons.gov/businessIndex/dev/lib/univocity-parsers-1.5.1.jar,hdfs://dev4/ons.gov/businessIndex/dev/lib/commons-csv-1.1.jar --driver-java-options "-Dbi-dataload.app-data.env=dev -Xms1g -Xmx5g"

## Further information ##

* [README](../README.md)

> * [File locations](./bi-dataload-file-locations.md).
> * [Step 0](./bi-dataload-step-0.md).
> * [Step 1](./bi-dataload-step-1.md).
> * [Step 2](./bi-dataload-step-2.md).
> * [Step 3](./bi-dataload-step-3.md).
> * [Testing](./bi-dataload-testing.md).
> * [CSV extract](./bi-dataload-csv-extract.md).
