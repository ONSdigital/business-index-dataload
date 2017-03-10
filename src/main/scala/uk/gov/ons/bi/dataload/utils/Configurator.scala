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
   * Allows us to manage configs via typed objects instead of raw strings.
   */

  private val config: Config = ConfigFactory.load()

  private lazy val root = config.getConfig("bi-dataload")

  def configPropertyNameAsEnv(name: String): String = {
    // Assumes property called bi-dataload.es.index would correspond to
    // an environment variable such as BI_DATALOAD_ES_INDEX, for example.
    name.toUpperCase.replaceAll("""\.""", "_").replaceAll("""-""", "_")
  }

  def envOrElseConfigStr(name: String, localConfig: Config = config): String = {
    val varName = configPropertyNameAsEnv(name)
    Properties.envOrElse(varName, localConfig.getString(name))
  }

  object ExtDataConfig {

    // allows us to pass sub-configs around separately

    private val extDataConfig = root.getConfig("ext-data")

    lazy val dir = envOrElseConfigStr("dir", extDataConfig)

    lazy val paye = envOrElseConfigStr("paye", extDataConfig)

    lazy val vat = envOrElseConfigStr("vat", extDataConfig)

    lazy val ch = envOrElseConfigStr("ch", extDataConfig)

    lazy val chDir = envOrElseConfigStr("ch-dir", extDataConfig)

    lazy val payeDir = envOrElseConfigStr("paye-dir", extDataConfig)

    lazy val vatDir = envOrElseConfigStr("vat-dir", extDataConfig)

  }

  object LinksDataConfig {

    private val linksDataConfig = root.getConfig("links-data")

    lazy val json = envOrElseConfigStr("json", linksDataConfig)

    lazy val dir = envOrElseConfigStr("dir", linksDataConfig)
  }

  object AppDataConfig {

    // allows us to pass sub-configs around separately

    private val appDataConfig = root.getConfig("app-data")

    // Apparently we are supposed to be able to write to dev/test/beta
    // directories under the main app data directory.
    lazy val env = envOrElseConfigStr("env", appDataConfig)

    // directories

    lazy val dir = envOrElseConfigStr("dir", appDataConfig)

    lazy val work = envOrElseConfigStr("work", appDataConfig)

    lazy val prev = envOrElseConfigStr("prev", appDataConfig)

    // files

    lazy val paye = envOrElseConfigStr("paye", appDataConfig)

    lazy val vat = envOrElseConfigStr("vat", appDataConfig)

    lazy val ch = envOrElseConfigStr("ch", appDataConfig)

    lazy val links = envOrElseConfigStr("links", appDataConfig)

    lazy val bi = envOrElseConfigStr("bi", appDataConfig)

    // Derive working/previous directories from above settings.
    // Saves having to replicate this in multiple places in code.
    lazy val (workingDir, prevDir) =
      if (env != "") (s"$dir/$env/$work", s"$dir/$env/$prev")
      else (s"$dir/$work", s"$dir/$prev")
  }

  object ESConfig {

    // allows us to pass sub-configs around separately

    private val esConfig = root.getConfig("es")

    lazy val nodes = envOrElseConfigStr("nodes", esConfig)

    lazy val port = envOrElseConfigStr("port", esConfig).toInt

    lazy val esUser = envOrElseConfigStr("es-user", esConfig)

    lazy val esPass = envOrElseConfigStr("es-pass", esConfig)

    lazy val index = envOrElseConfigStr("index", esConfig)

    lazy val indexType = envOrElseConfigStr("index-type", esConfig)

    lazy val autocreate = envOrElseConfigStr("autocreate", esConfig)

    lazy val wanOnly = envOrElseConfigStr("wan-only", esConfig)

    override def toString: String = {
      s"""[nodes = $nodes,
         | port = $port,
         | user = $esUser,
         | pass = $esPass,
         | index = $index,
         | index-type = $indexType,
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


