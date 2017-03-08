package uk.gov.ons.bi.dataload.utils

import com.google.inject.Singleton

import scala.util.Properties
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by websc on 03/02/2017.
  */
@Singleton
class AppConfig {

  /*
   * Allows us to manage configs via objects instead of raw strings.
   */

  private val config: Config = ConfigFactory.load()

  private lazy val root = config.getConfig("dataload")

  def configPropertyNameAsEnv(name: String): String = {
    // Assumes property called dataload.es.index would correspond to
    // an environment variable such as DATALOAD_ES_INDEX, for example.
    name.toUpperCase.replaceAll("""\.""", "_").replaceAll("""-""", "_")
  }

  def envOrElseConfigStr(name: String, localConfig: Config = config): String = {
    val varName = configPropertyNameAsEnv(name)
    Properties.envOrElse(varName, localConfig.getString(name))

  }

  object SourceDataConfig {

    // allows us to pass sub-configs around separately

    private val sourceDataConfig = root.getConfig("src-data")

    lazy val dir = envOrElseConfigStr("dir", sourceDataConfig)

    lazy val paye = envOrElseConfigStr("paye", sourceDataConfig)

    lazy val vat = envOrElseConfigStr("vat", sourceDataConfig)

    lazy val ch = envOrElseConfigStr("ch", sourceDataConfig)

    lazy val links = envOrElseConfigStr("links", sourceDataConfig)

    lazy val chDir = envOrElseConfigStr("ch-dir", sourceDataConfig)

    lazy val payeDir = envOrElseConfigStr("paye-dir", sourceDataConfig)

    lazy val vatDir = envOrElseConfigStr("vat-dir", sourceDataConfig)

    lazy val linksDir = envOrElseConfigStr("links-dir", sourceDataConfig)

  }

  object ParquetDataConfig {

    // allows us to pass sub-configs around separately

    private val parquetDataConfig = root.getConfig("parquet-data")

    lazy val dir = envOrElseConfigStr("dir", parquetDataConfig)

    lazy val paye = envOrElseConfigStr("paye", parquetDataConfig)

    lazy val vat = envOrElseConfigStr("vat", parquetDataConfig)

    lazy val ch = envOrElseConfigStr("ch", parquetDataConfig)

    //lazy val newLinks = envOrElseConfigStr("new-links", parquetDataConfig)

    // This is for the old links file from last run
    //lazy val oldLinks = envOrElseConfigStr("old-links", parquetDataConfig)

    lazy val links = envOrElseConfigStr("links", parquetDataConfig)

    lazy val prevDir = envOrElseConfigStr("prev-dir", parquetDataConfig)

    lazy val bi = envOrElseConfigStr("bi", parquetDataConfig)
  }

  object ESConfig {

    // allows us to pass sub-configs around separately

    private val esConfig = root.getConfig("es")

    lazy val nodes = envOrElseConfigStr("nodes", esConfig)

    lazy val port = envOrElseConfigStr("port", esConfig).toInt

    lazy val esUser = envOrElseConfigStr("es-user", esConfig)

    lazy val esPass = envOrElseConfigStr("es-pass", esConfig)

    lazy val index = envOrElseConfigStr("index", esConfig)

    lazy val autocreate = envOrElseConfigStr("autocreate", esConfig)

    lazy val wanOnly = envOrElseConfigStr("wan-only", esConfig)

    override def toString: String = {
      s"""[nodes = $nodes,
         | port = $port,
         | user = $esUser,
         | pass = $esPass,
         | index = $index,
         | autocreate = $autocreate,
         | wanOnly = $wanOnly
         | ]
        """.stripMargin
    }
  }

  object SparkConfigInfo {

    private val sparkConfig = root.getConfig("spark")

    lazy val appName = envOrElseConfigStr("app-name", sparkConfig)

    lazy val serializer = envOrElseConfigStr("serializer", sparkConfig)

  }

}


