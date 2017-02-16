package uk.gov.ons.bi.dataload.linker

import com.google.inject.Singleton
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import uk.gov.ons.bi.dataload.utils.AppConfig
import uk.gov.ons.bi.dataload.model._
import uk.gov.ons.bi.dataload.reader.ParquetReader

/**
  * Created by websc on 16/02/2017.
  */
@Singleton
class LinkedBusinessBuilder(appConfig: AppConfig)(implicit val sc: SparkContext) {

  val pqReader = new ParquetReader

  val companies: RDD[CompanyRec] = pqReader.loadCompanyRecsFromParquet(appConfig)

  val links: RDD[LinkRec] = pqReader.loadLinkRecsFromParquet(appConfig)

}
