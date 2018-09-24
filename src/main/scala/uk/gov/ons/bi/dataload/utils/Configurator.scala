package uk.gov.ons.bi.dataload.utils

import com.google.inject.Singleton
import com.typesafe.config.{Config, ConfigFactory}

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

  def getLocalHome: String = {
    val external = getClass.getClassLoader.getResource("external").toString
    val split = external.split("external")
    split(0)
  }

  object home {
    lazy val cluster = getConfigStr("cluster", root)

    lazy val env = cluster match {
      case "local" => getLocalHome
      case "cluster" => s"/${getConfigStr("env", root)}"
    }

    override def toString: String = {
      s"""[
      | env = $env,
      | cluster = $cluster
      | ]
      """.stripMargin
    }
  }

  object External {
    private val extConfig = root.getConfig("external")

    lazy val extDir = getConfigStr("ext-dir", extConfig)

    lazy val chDir = getConfigStr("ch-dir", extConfig)

    lazy val vatDir = getConfigStr("vat-dir", extConfig)

    lazy val payeDir = getConfigStr("paye-dir", extConfig)

    lazy val lookupsDir = getConfigStr("lookups-dir", extConfig)

    lazy val ch = getConfigStr("ch", extConfig)

    lazy val vat = getConfigStr("vat", extConfig)

    lazy val paye = getConfigStr("paye", extConfig)

    lazy val tcnToSic = getConfigStr("tcn-to-sic", extConfig)

    lazy val externalPath = s"${home.env}/$extDir"

    override def toString: String = {
      s"""[
         | ext-dir = $extDir,
         | ch-dir = $chDir,
         | paye-dir = $payeDir,
         | vat-dir = $vatDir
         | lookups-dir = $lookupsDir,
         | paye = $paye,
         | vat= $vat,
         | ch = $ch,
         | tcn-to-sic = $tcnToSic
         | ]
        """.stripMargin
    }
  }

  object BusinessIndex {
    private val biConfig = root.getConfig("businessIndex")

    // Business Index directory config

    lazy val biDir = getConfigStr("bi-dir", biConfig)

    lazy val elasticDir = getConfigStr("elastic-dir", biConfig)

    lazy val previousDir = getConfigStr("previous-dir", biConfig)

    lazy val extractDir = getConfigStr("extract-dir", biConfig)

    lazy val workingDir = getConfigStr("working-data-dir", biConfig)

    lazy val dataScienceDir = getConfigStr("data-science-dir", biConfig)

    lazy val dataScienceFile = getConfigStr("data-science", biConfig)

    // Business Index file config
    val bi = getConfigStr("bi", biConfig)
    val links = getConfigStr("links", biConfig)
    val ch = getConfigStr("ch", biConfig)
    val vat = getConfigStr("vat", biConfig)
    val paye = getConfigStr("paye", biConfig)
    val tcn = getConfigStr("tcn", biConfig)

    val biPath = s"${home.env}/$biDir"
    val workPath = s"$biPath/$workingDir"
    val prevPath = s"$biPath/$previousDir"

    override def toString: String = {
      s"""[
         | elastic-dir = $elasticDir,
         | previous-dir = $previousDir,
         | extract-dir = $extractDir,
         | working-data-dir = $workingDir,
         | data-science-dir = $dataScienceDir,
         | data-science = $dataScienceFile,
         | bi = $bi,
         | links = $links,
         | ch = $ch,
         | vat = $vat,
         | paye = $paye,
         | tcn = $tcn,
         | bi-dir = $biDir
         |]
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


