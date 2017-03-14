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

  def getConfigStr(name: String, localConfig: Config = config): String = {
    localConfig.getString(name)
  }

  object ExtDataConfig {

    // allows us to pass sub-configs around separately

    private val extDataConfig = root.getConfig("ext-data")

    lazy val dir = getConfigStr("dir", extDataConfig)

    lazy val paye = getConfigStr("paye", extDataConfig)

    lazy val vat = getConfigStr("vat", extDataConfig)

    lazy val ch = getConfigStr("ch", extDataConfig)

    lazy val chDir = getConfigStr("ch-dir", extDataConfig)

    lazy val payeDir = getConfigStr("paye-dir", extDataConfig)

    lazy val vatDir = getConfigStr("vat-dir", extDataConfig)

  }

  object LinksDataConfig {

    private val linksDataConfig = root.getConfig("links-data")

    lazy val json = getConfigStr("json", linksDataConfig)

    lazy val dir = getConfigStr("dir", linksDataConfig)
  }

  object AppDataConfig {

    // allows us to pass sub-configs around separately

    private val appDataConfig = root.getConfig("app-data")

    // Apparently we are supposed to be able to write to dev/test/beta
    // directories under the main app data directory.
    lazy val env = getConfigStr("env", appDataConfig)

    // directories

    lazy val dir = getConfigStr("dir", appDataConfig)

    lazy val work = getConfigStr("work", appDataConfig)

    lazy val prev = getConfigStr("prev", appDataConfig)

    // files

    lazy val paye = getConfigStr("paye", appDataConfig)

    lazy val vat = getConfigStr("vat", appDataConfig)

    lazy val ch = getConfigStr("ch", appDataConfig)

    lazy val links = getConfigStr("links", appDataConfig)

    lazy val bi = getConfigStr("bi", appDataConfig)

    // Derive working/previous directories from above settings.
    // Saves having to replicate this in multiple places in code.
    lazy val (workingDir, prevDir) =
    if (env != "") (s"$dir/$env/$work", s"$dir/$env/$prev")
    else (s"$dir/$work", s"$dir/$prev")
  }

  object ESConfig {

    // allows us to pass sub-configs around separately

    private val esConfig = root.getConfig("es")

    lazy val nodes = getConfigStr("nodes", esConfig)

    lazy val port = getConfigStr("port", esConfig).toInt

    lazy val esUser = getConfigStr("es-user", esConfig)

    lazy val esPass = getConfigStr("es-pass", esConfig)

    lazy val index = getConfigStr("index", esConfig)

    lazy val indexType = getConfigStr("index-type", esConfig)

    lazy val autocreate = getConfigStr("autocreate", esConfig)

    lazy val wanOnly = getConfigStr("wan-only", esConfig)

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

    lazy val appName = getConfigStr("app-name", sparkConfig)

    lazy val serializer = getConfigStr("serializer", sparkConfig)

  }

}


