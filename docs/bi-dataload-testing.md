# BI Dataload Testing #

## Testing libraries ##

* [Scala Test](http://www.scalatest.org/): standard Scala testing library.
* [Spark Testing Base](https://github.com/holdenk/spark-testing-base): a 3rd party Spark package that provides useful support for testing Spark with Scala Test.
* [SBT Spark Package Plugin](https://github.com/databricks/sbt-spark-package): (removed) an SBT plugin that makes it easier to include and use Spark packages with SBT.

## Testing approach ##

* We have **not** implemented full test coverage for Spark code in this application.
* Much of the functionality of this application is based on standard Spark operations, so there is not much point in testing standard Spark code.
* Spark applications typically consist of **transformations** that manipulate data and **actions** that typically perform reduce operations and will cause data to be materialised at runtime.

### Testing non-Spark code ###

* We have supplied examples of automated tests for most of the main transformations.
* These include functions that extract specific fields from e.g. Company data, or translate data values to coded values.
* In most cases, these functions do use any Spark-specific code, so we can test them with plain Scala Test.

### Testing Spark-based code (removed) ###

* Some operations depend on Spark-specific code.
* These include transformations from one RDD or DataFrame to another.
* We have provided some tests for these operations, and these tests make use of the Spark Testing Base library as well as Scala Test.
* These tests require a SparkContext to be instantiated, which is provided (in Spark local mode) via the Spark Testing Base functionality.
* Unfortunately we could not build or run these tests on Jenkins, so we had to remove the Spark testing code.
* This note is left in the docs so we have a starting point to create new Spark tests if possible in future.

## Running tests ##

* All tests in the current implementation can be run from the command-line using SBT:

> `sbt test`

* They can also be executed from inside IntelliJ IDEA if necessary.



## Further information ##

* [README](../README.md)

> * [File locations](./bi-dataload-file-locations.md).
> * [Step 0](./bi-dataload-step-0.md).
> * [Step 1](./bi-dataload-step-1.md).
> * [Step 2](./bi-dataload-step-2.md).
> * [Step 3](./bi-dataload-step-3.md).
> * [Testing](./bi-dataload-testing.md).
> * [CSV extract](./bi-dataload-csv-extract.md).
