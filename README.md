# ONS Business Indexes - data ingestion ##

## Purpose ##

* This application performs the data ingestion stage of the ONS Business Index data-flow.
* Source data files are received on a monthly basis.
* These are then processed by a separate machine learning application which attempts to match data for a given business from the different sources i.e. Companies House, HMRC/PAYE, HMRC/VAT.
* The ML process generates a file of possible links (currently in JSON format). 
* The data ingestion process consumes the links and source data files and generates corresponding entries for an ElasticSearch index of businesses.
* The ElasticSearch index is used by a separate application to support queries for business data.
* See [step 1 processing](./docs/bi-dataload-step-1.md) for more information on data sources and initial data-load.

## Platform ##

### Cloudera Hadoop ###

* The ONS Cloudera cluster was mandated as the core platform for the BI data ingestion process.
* This application uses the following components:

> * Apache Spark (currently version 1.6.0)
> * HDFS
> * Hue browser interface
> * Oozie
> * Oozie/Spark action template

* Application code and supporting libraries are held as JAR files in HDFS.
* This allows Oozie to access the code without having to install it on specific nodes within the cluster.

### Spark 1.6.0 ###

* We use Apache Spark for all the Cloudera-based processing.
* Spark provides well-integrated tools for reading, processing and storing data on a distributed platform, such as Hadoop.
* Parquet files are used for interim storage, as Parquet is optimised for performance and Spark can make intelligent use of this format.
* The Spark CSV package is used for reading the source CSV files.
* Spark CSV has to be provided separately for Spark 1.6.0, but will be included in the default installation of Spark 2.x.

### Scala 2.10 ###

* Apache Spark 1.6.0 on Cloudera is compiled for Scala 2.10.
* This means we also need to use Scala 2.10 for our application.
* When the Cloudera Spark installation is upgraded to Spark 2.x, we may need to re-compile the Scala code with Scala 2.11, which is the default Scala version for Spark 2.x.
* You can do this by changing the Scala version in the `build.sbt` file.

### ElasticSearch 2.3.x ###

* We are currently using an ElasticSearch cluster which is at version 2.3.
* If this cluster is upgraded to ElasticSearch 5.x, we may need to modify the Scala code to use the corresponding version of the ES/Spark API library.

### Additional libraries ###

* We use a couple of extra Scala and Java libraries.
* Spark CSV:

> * `spark-csv_2.10-1.5.0.jar`: Spark CSV package
> * `commons-csv-1.1.jar`: Additional dependency for Spark CSV
> * `univocity-parsers-1.5.1.jar`: Additional dependency for Spark CSV

* ElasticSearch Spark API:

> * `elasticsearch-spark_2.10-2.4.4.jar`

* These are provided as JARs at runtime (see individual steps below for more information).
* See the `build.sbt` file for Maven/SBT artifact details.

## Building the application ##

* Build the application using SBT from the command-line:

> `sbt clean compile package`

* We use the SBT `package` task, not `assembly`, because we do not want to build a fat JAR here.
* Cloudera has its own installation of Spark, so we do not want to deploy our application with all the Spark libraries.
* It can be difficult to build an assembly package without introducing conflicts between the various Spark libraries and their dependencies.
* The easiest option is to just build the basic package here, then providing the extra JARS as `--jars` dependencies at runtime.

## Deploying the application ##

* The initial implementation is deployed manually.
* The application JAR and the 3rd party library JARs are all placed in a directory in HDFS that can be accessed by Oozie.
* Data files are also stored in HDFS.
* The data file names and locations are specified in `application.conf`, but can be provided at runtime via "-D" parameters.
* The application is executed as a 3-step Oozie workflow.
* The individual steps and their Oozie task parameters are documented separately below.


## Data file locations ##

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


## Detailed processing ##

* [Step 1: Load raw data to parquet files](./docs/bi-dataload-step-1.md)

* [Step 2: Build business index entries](./docs/bi-dataload-step-2.md)

* [Step 3: Upload business index entries to ElasticSearch](./docs/bi-dataload-step-3.md)