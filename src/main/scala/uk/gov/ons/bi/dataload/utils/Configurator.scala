package uk.gov.ons.bi.dataload.utils

import com.google.inject.Singleton
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by websc on 03/02/2017.
  */
@Singleton
class AppConfig {

  /*
   * Allows us to manage configs via typed objects instead of raw strings.
   */

  private val config: Config = ConfigFactory.load()

  private lazy val root = config.getConfig("bi-dataload")

  def getConfigStr(name: String, localConfig: Config = config): String = {
    localConfig.getString(name)
  }

  object ExtDataConfig {

    // allows us to pass sub-configs around separately

    private val localConfig = root.getConfig("ext-data")

    lazy val env = getConfigStr("env", localConfig)

    lazy val dir = getConfigStr("dir", localConfig)

    lazy val paye = getConfigStr("paye", localConfig)

    lazy val vat = getConfigStr("vat", localConfig)

    lazy val ch = getConfigStr("ch", localConfig)

    lazy val chDir = getConfigStr("ch-dir", localConfig)

    lazy val payeDir = getConfigStr("paye-dir", localConfig)

    lazy val vatDir = getConfigStr("vat-dir", localConfig)

    override def toString: String = {
      s"""[env = $env
         | dir = $dir,
         | paye = $paye,
         | vat= $vat,
         | ch = $ch,
         | ch-dir = $chDir,
         | paye-dir = $payeDir,
         | vat-dir = $vatDir
         | ]
        """.stripMargin
    }
  }

  object OnsDataConfig {

    private val onsDataConfig = root.getConfig("ons-data")

    val baseDir = getConfigStr("dir", onsDataConfig)

    val linksDataConfig  = new {

      private val linksConfig = onsDataConfig.getConfig("links")

      // Links dir is below ONS data dir
      private val localDir = getConfigStr("dir", linksConfig)
      // We provide the full path
      //val dir = s"/$baseDir/$localDir" This is the original version but we are substituting it so that we can use the /user/bi-dev-ci directory
      val dir = s"/$localDir" // Incorrect value, replace with version above once /dev/ons.gov/businessIndex/links exists

      val parquet = getConfigStr("parquet", linksConfig)

      override def toString: String = {
        s"""[parquet = $parquet,
           | dir = $dir
           | ]
      """.stripMargin
      }
    }

    val lookupsConfig = new {
      private val lookupsConfig = onsDataConfig.getConfig("lookups")

      // Lookups dir is below ONS data dir
      private val localDir = getConfigStr("dir", lookupsConfig)
      // We provide the full path
      val dir = s"/$baseDir/$localDir"

      val tcnToSic = getConfigStr("tcn-to-sic", lookupsConfig)

      override def toString: String = {
        s"""[tcn-to-sic = $tcnToSic,
           | dir = $dir
           | ]
        """.stripMargin
      }
    }
  }

  object AppDataConfig {

    // allows us to pass sub-configs around separately

    private val localConfig = root.getConfig("app-data")

    // Apparently we are supposed to be able to write to dev/test/beta
    // directories under the main app data directory.
    lazy val env = getConfigStr("env", localConfig)

    // directories

    lazy val dir = getConfigStr("dir", localConfig)

    lazy val work = getConfigStr("work", localConfig)

    lazy val prev = getConfigStr("prev", localConfig)

    lazy val extract = getConfigStr("extract", localConfig)

    // files

    lazy val paye = getConfigStr("paye", localConfig)

    lazy val vat = getConfigStr("vat", localConfig)

    lazy val ch = getConfigStr("ch", localConfig)

    lazy val links = getConfigStr("links", localConfig)

    lazy val bi = getConfigStr("bi", localConfig)

    lazy val tcn = getConfigStr("tcn", localConfig)


    // Derive working/previous directories from above settings.
    // Saves having to replicate this in multiple places in code.
    lazy val (workingDir, prevDir) =
    if (env != "") (s"/$env/$dir/$work", s"/$env/$dir/$prev")
    else (s"$dir/$work", s"$dir/$prev")

    override def toString: String = {
      s"""[env = $env,
         | dir = $dir,
         | work = $work,
         | prev = $prev,
         | extract = $extract,
         | paye = $paye,
         | vat= $vat,
         | ch = $ch,
         | links = $links,
         | bi = $bi,
         | tcn = $tcn
         | ]
        """.stripMargin
    }

  }

  object ESConfig {

    // allows us to pass sub-configs around separately

    private val localConfig = root.getConfig("es")

    lazy val nodes = getConfigStr("nodes", localConfig)

    lazy val port = getConfigStr("port", localConfig).toInt

    lazy val esUser = getConfigStr("es-user", localConfig)

    lazy val esPass = getConfigStr("es-pass", localConfig)

    lazy val index = getConfigStr("index", localConfig)

    lazy val indexType = getConfigStr("index-type", localConfig)

    lazy val autocreate = getConfigStr("autocreate", localConfig)

    lazy val wanOnly = getConfigStr("wan-only", localConfig)

    lazy val parquetDir = getConfigStr("parquet-dir", localConfig)

    override def toString: String = {
      s"""[nodes = $nodes,
         | port = $port,
         | user = $esUser,
         | pass = $esPass,
         | index = $index,
         | index-type = $indexType,
         | autocreate = $autocreate,
         | wanOnly = $wanOnly,
         | parquetDir = $parquetDir
         | ]
        """.stripMargin
    }
  }

  object SparkConfigInfo {

    private val localConfig = root.getConfig("spark")

    lazy val appName = getConfigStr("app-name", localConfig)

    lazy val serializer = getConfigStr("serializer", localConfig)

    override def toString: String = {
      s"""[app-name = $appName,
         | serializer = $serializer
         | ]
        """.stripMargin
    }
  }

}


