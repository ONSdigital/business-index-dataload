package uk.gov.ons.bi.dataload.utils

import com.google.inject.Singleton
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by websc on 28/04/2017.
  */
@Singleton
class ContextMgr(sparkConf: SparkConf = new SparkConf()) {

  implicit val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

  implicit val sqlContext = new HiveContext(sc)
}
