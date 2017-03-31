package uk.gov.ons.bi.dataload

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.apache.spark.sql.SQLContext

trait SparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  // Provides a SparkContext we can mix in for local testing.
  // (based on tutorial code https://github.com/mkuthan/example-spark)

  // Use vars because we need to set these later and make tehm available to tests
  private var _sc: SparkContext = _

  // Allows us to provide extra Spark config
  def sparkConfig: Map[String, String] = Map.empty

  override def beforeAll(): Unit = {
    super.beforeAll()

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)

    // include any extra config
    sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

    _sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    super.afterAll()
  }

  // provides default SparkContext to tests as "sc"
  def sc: SparkContext = this._sc

}


trait SparkSqlSpec extends SparkSpec {
  this: Suite =>

  private var _sqlc: SQLContext = _

  def sqlc: SQLContext = _sqlc

  override def beforeAll(): Unit = {
    super.beforeAll()

    _sqlc = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    _sqlc = null

    super.afterAll()
  }

}