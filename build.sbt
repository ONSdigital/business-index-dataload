import com.typesafe.config.ConfigFactory
import sbtassembly.AssemblyPlugin.autoImport._


lazy val configPath = System.getProperty("config.path")
lazy val appConfig = ConfigFactory.parseFile(new File( "src/main/resources/application.conf")).resolve().getConfig("bi-dataload")

// key-bindings
lazy val ITest = config("it") extend Test

lazy val Versions = new {
  val spark = "1.6.0"
  val sparkCsv = "1.5.0"
  val joda = "2.9.4"
  val jodaConvert = "1.8.1"
  val json4s = "3.5.0"
  val es = "2.4.4"
  val scala = "2.10.6"
}

lazy val Constant = new {
  val moduleName = """business-index-dataload"""
  val projectStage = "alpha"
  val organisation = "ons"
  val team = "bi"
  val local = "mac"

  val publishTrigger: Boolean = appConfig.getBoolean("artifactory.publish-init")
  val publishRepo: String = appConfig.getString("artifactory.publish-repository")
  val artifactoryHost: String = appConfig.getString("artifactory.host")
  val artifactoryUser: String = appConfig.getString("artifactory.user")
  val artifactoryPassword: String = appConfig.getString("artifactory.password")
}


lazy val Resolvers = Seq(
  Resolver.typesafeRepo("releases"),
  "conjars" at "http://conjars.org/repo",
  "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven/"
)

lazy val testSettings = Seq(
  sourceDirectory in ITest := baseDirectory.value / "/test/it",
  resourceDirectory in ITest := baseDirectory.value / "/test/resources",
  scalaSource in ITest := baseDirectory.value / "test/it",
  // test setup
  parallelExecution in Test := false
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {}
)

lazy val publishingSettings = Seq(
  publishArtifact := Constant.publishTrigger,
  publishMavenStyle := false,
  checksums in publish := Nil,
  publishArtifact in Test := false,
  publishArtifact in (Compile, packageBin) := false,
  publishArtifact in (Compile, packageSrc) := false,
  publishArtifact in (Compile, packageDoc) := false,
  publishTo := {
    if (System.getProperty("os.name").toLowerCase.startsWith(Constant.local) )
      Some(Resolver.file("file", new File(s"${System.getProperty("user.home").toLowerCase}/Desktop/")))
    else
      Some("Artifactory Realm" at Constant.publishRepo)
  },
  artifact in (Compile, assembly) ~= { art =>
    art.copy(`type` = "jar", `classifier` = Some("assembly"))
  },
  artifactName := { (sv: ScalaVersion, module: ModuleID, artefact: Artifact) =>
    module.organization + "_" + artefact.name + "-" + artefact.classifier.getOrElse("package") + "-" + module.revision + "." + artefact.extension
  },
  credentials += Credentials("Artifactory Realm", Constant.artifactoryHost, Constant.artifactoryUser, Constant.artifactoryPassword),
  releaseTagComment := s"Releasing $name ${(version in ThisBuild).value}",
  releaseCommitMessage := s"Setting Release tag to ${(version in ThisBuild).value}",
  // no commit - ignore zip and other package files
  releaseIgnoreUntrackedFiles := true
)


lazy val commonSettings = Seq (
  scalaVersion := Versions.scala,
  scalacOptions in ThisBuild ++= Seq(
    "-language:experimental.macros",
    //"-target:jvm-1.8",
    "-encoding", "UTF-8",
    "-language:reflectiveCalls",
    "-language:experimental.macros",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:postfixOps",
    "-deprecation", // warning and location for usages of deprecated APIs
    "-feature", // warning and location for usages of features that should be imported explicitly
    "-unchecked", // additional warnings where generated code depends on assumptions
    "-Xlint", // recommended additional warnings
    "-Xcheckinit", // runtime error when a val is not initialized due to trait hierarchies (instead of NPE somewhere else)
    "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver
    //"-Yno-adapted-args", // Do not adapt an argument list (either by inserting () or creating a tuple) to match the receiver
    "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
    "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures
    "-Ywarn-dead-code", // Warn when dead code is identified
    //"-Ywarn-unused", // Warn when local and private vals, vars, defs, and types are unused
    //"-Ywarn-unused-import", //  Warn when imports are unused (don't want IntelliJ to do it automatically)
    "-Ywarn-numeric-widen" // Warn when numerics are widened
  ),
  resolvers ++= Resolvers ++ Seq("Artifactory" at s"${Constant.publishRepo}"),
  coverageExcludedPackages := ".*Routes.*;.*ReverseRoutes.*;.*javascript.*"
)


lazy val load = (project in file("."))
  .configs(ITest)
  .settings(inConfig(ITest)(Defaults.testSettings) : _*)
  .settings(commonSettings: _*)
  .settings(testSettings:_*)
  .settings(publishingSettings:_*)
  .settings(noPublishSettings:_*)
  .settings(
    moduleName := Constant.moduleName,
    organizationName := Constant.organisation,
    description := "<description>",
    version := (version in ThisBuild).value,
    name := s"${organizationName.value}-${moduleName.value}",
    licenses := Seq("MIT-License" -> url("https://github.com/ONSdigital/sbr-control-api/blob/master/LICENSE")),
    startYear := Some(2016),
    libraryDependencies ++= Seq (
      "org.apache.spark"      %%     "spark-core"          % Versions.spark      % "provided",
      "org.apache.spark"      %%     "spark-sql"           % Versions.spark      % "provided",
      "org.elasticsearch"     %%     "elasticsearch-spark" % Versions.es         % "provided",
      // Change this to another test framework if you prefer
      "org.scalatest"         %%     "scalatest"           % "2.2.4"             % "test",
      // Restore this when the Cloudera bug is fixed so we can use HiveContext for UBRN rules
      // "org.apache.spark" %% "spark-hive" % Versions.spark % "provided",
      "joda-time"             %      "joda-time"           % Versions.joda,
      "org.joda"              %      "joda-convert"        % Versions.jodaConvert
        excludeAll ExclusionRule(organization = "javax.servlet")
    ),
    // assembly
    assemblyJarName in assembly := s"${Constant.organisation}_${Constant.moduleName}-assembly-${version.value}.jar",
    assemblyMergeStrategy in assembly := {
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )


