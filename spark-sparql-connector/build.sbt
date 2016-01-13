name := "spark-sparql-connector"

version := "1.0.0-SNAPSHOT"

organization := "de.usu.research"

scalaVersion := "2.11.7"

spName := "USU-Research/spark-sparql-connector"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.5.2"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

sparkComponents := Seq("core", "sql")

libraryDependencies ++= Seq(
  "org.apache.jena" % "jena-jdbc-driver-remote" % "3.0.1",
  "org.apache.jena" % "jena-jdbc-driver-mem"    % "3.0.1",
  "org.slf4j"       % "slf4j-api"               % "1.7.5" % Provided,
  "org.scalatest"  %% "scalatest"               % "2.2.1" % "test"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" force(),
  "org.scala-lang" % "scala-library" % scalaVersion.value % Provided
)

// This is necessary because of how we explicitly specify Spark dependencies
// for tests rather than using the sbt-spark-package plugin to provide them.
spIgnoreProvided := true

publishMavenStyle := true

spAppendScalaVersion := true

spIncludeMaven := true

pomExtra := (
  <url>https://github.com/databricks/spark-csv</url>
  <licenses>
<!--  
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
    -->
  </licenses>
  <scm>
    <url>git@github.com:USU-Research/spark-sparql-connector.git</url>
    <connection>scm:git:git@github.com:USU-Research/spark-sparql-connector.git</connection>
  </scm>
  <developers>
    <developer>
      <id>usimweinde</id>
      <name>Martin Weindel</name>
    </developer>
  </developers>)

parallelExecution in Test := false

// Skip tests during assembly
test in assembly := {}

assemblyJarName in assembly := name.value + "-spark" + sparkVersion.value + "-scala" + scalaVersion.value.substring(0,4) + "-" + version.value + ".jar"

excludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { x => x.data.getName.contains("slf4j") ||
   x.data.getName.contains("log4j") ||
   x.data.getName.startsWith("scala-library") || 
   x.data.getName.startsWith("jackson-") 
  }
}
ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}


// ------------------------------------------------------------------------------------------------