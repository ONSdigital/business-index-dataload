package uk.gov.ons.bi.dataload.utils

import scala.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by websc on 03/02/2017.
  */
class AppConfig  {

  private val config: Config = ConfigFactory.load()

  private lazy val root = config.getConfig("dataload")

  def configPropertyNameAsEnv(name: String): String = {
    // Assumes env variable for my-app.es-config.host would be
    // MY_APP_ES_CONFIG_HOST:
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

  }

  object ParquetDataConfig {

    // allows us to pass sub-configs around separately

    private val parquetDataConfig = root.getConfig("parquet-data")

    lazy val dir = envOrElseConfigStr("dir", parquetDataConfig)

    lazy val paye = envOrElseConfigStr("paye", parquetDataConfig)

    lazy val vat = envOrElseConfigStr("vat", parquetDataConfig)

    lazy val ch = envOrElseConfigStr("ch", parquetDataConfig)

    lazy val links = envOrElseConfigStr("links", parquetDataConfig)
  }

  object ESConfig {

    // allows us to pass sub-configs around separately

    private val esConfig = root.getConfig("es-config")

    lazy val host = envOrElseConfigStr("host", esConfig)

    lazy val port = envOrElseConfigStr("port", esConfig).toInt

    lazy val reps = envOrElseConfigStr("reps", esConfig).toInt
  }
}


