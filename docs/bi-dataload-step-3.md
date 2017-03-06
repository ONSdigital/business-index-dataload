# BI Dataload step 3: upload Business Index entries to ElasticSearch #


* [README](../README.md)

> * [Step 0](./bi-dataload-step-0.md).
> * [Step 1](./bi-dataload-step-1.md).
> * [Step 2](./bi-dataload-step-2.md).
> * [Step 3](./bi-dataload-step-3.md).

## What? ##

* This step reads a single Parquet file of Business Index entries that was generated in [Step 2](./bi-dataload-step-2.md).
* It then uploads the BI entries to the specified index in ElasticSearch.

## How? ##

### Data file locations ###

* The Parquet files are all stored in a specified working data directory
* See [step 1](./bi-dataload-ste-1.md) for default file locations.

### ElasticSearch processing ###

* This step uses the ElasticSearch Spark API.
* This library needs to be provided as a JAR file at runtime.

> * `elasticsearch-spark_2.10-2.4.4.jar`

* The JAR should be stored in HDFS so that it can be loaded by the Oozie job at runtime.
* The ElasticSearch node IP address and index name are specified via configuration parameters.
* These can be provided to the Oozie task at runtime:

> `-Ddataload.es.index=bi-dev -Ddataload.es.nodes=127.0.0.1`
 
* These values are used by the ElasticSearch Spark API when writing to the ES index.

### Oozie task specification ###

* We use Oozie to execute the Spark processing on Cloudera.
* Each step is defined as a separate task in the work-flow.
* Step 3 should be defined as indicated below.

#### Oozie Task Definition ####

* Assumes files are installed in HDFS `hdfs://dev4/user/appUser`.
* Performance may depend on ElasticSearch cluster resources which are outside the scope of this application.
* This example specifies 8 Spark executors.
* It may be possible to tweak the various Spark settings to use less memory etc, but this configuration seems to work OK with current data-sets.

Page 1 Field | Contents
------------- | -------------
Spark Master  | yarn-cluster
Mode  | cluster
App Name | ONS BI Dataload Step 3 Upload to ElasticSearch
Jars/py files | hdfs://dev4/user/appUser/libs/business-index-dataload_2.10-1.0.jar
Main class | uk.gov.ons.bi.dataload.LoadBiToEsApp

Page 2 Field | Contents
------------- | -------------
Properties / Options list | --driver-memory 4G --num-executors 8 --executor-memory 3G --jars hdfs://dev4/user/appUser/libs/elasticsearch-spark_2.10-2.4.4.jar --driver-java-options "-Xms1g -Xmx4g -Ddataload.es.index=bi-dev -Ddataload.es.nodes=127.0.0.1"

-----
