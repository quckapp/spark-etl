import Dependencies._

ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.quickchat"

lazy val root = (project in file("."))
  .settings(
    name := "quickchat-spark-etl",

    // Spark dependencies
    libraryDependencies ++= Seq(
      // Spark Core
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

      // Spark Streaming Kafka
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

      // Delta Lake
      "io.delta" %% "delta-core" % "2.4.0",
      "io.delta" %% "delta-storage" % "2.4.0",

      // MongoDB Spark Connector
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.1",

      // MySQL Connector
      "mysql" % "mysql-connector-java" % "8.0.33",

      // AWS S3
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.560",

      // Configuration
      "com.typesafe" % "config" % "1.4.3",
      "com.github.pureconfig" %% "pureconfig" % "0.17.4",

      // JSON Processing
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",

      // Logging
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.4.11",

      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test
    ),

    // Assembly settings
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    },

    assembly / assemblyJarName := "quickchat-spark-etl.jar",

    // Scala compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code"
    ),

    // Test settings
    Test / fork := true,
    Test / parallelExecution := false
  )

lazy val sparkVersion = "3.5.0"
