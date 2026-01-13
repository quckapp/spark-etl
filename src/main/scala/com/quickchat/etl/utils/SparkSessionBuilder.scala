package com.quickchat.etl.utils

import com.quickchat.etl.config.AppConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object SparkSessionBuilder extends LazyLogging {

  def build(config: AppConfig): SparkSession = {
    logger.info(s"Building SparkSession for ${config.app.name}")

    val builder = SparkSession.builder()
      .master(config.spark.master)
      .appName(config.spark.appName)

    // Apply Spark configurations
    config.spark.config.foreach { case (key, value) =>
      builder.config(key, value)
    }

    // Configure Delta Lake
    builder
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    // Configure AWS S3 if credentials provided
    if (config.aws.accessKey.nonEmpty) {
      builder
        .config("spark.hadoop.fs.s3a.access.key", config.aws.accessKey)
        .config("spark.hadoop.fs.s3a.secret.key", config.aws.secretKey)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

      // Configure custom S3 endpoint (for MinIO, LocalStack, etc.)
      if (config.aws.s3.endpoint.nonEmpty) {
        builder
          .config("spark.hadoop.fs.s3a.endpoint", config.aws.s3.endpoint)
          .config("spark.hadoop.fs.s3a.path.style.access", config.aws.s3.pathStyleAccess.toString)
      }
    }

    // MongoDB configurations
    builder
      .config("spark.mongodb.read.connection.uri", config.mongodb.uri)
      .config("spark.mongodb.write.connection.uri", config.mongodb.uri)
      .config("spark.mongodb.read.database", config.mongodb.database)
      .config("spark.mongodb.write.database", config.mongodb.database)

    val spark = builder.getOrCreate()

    logger.info(s"SparkSession created successfully. Version: ${spark.version}")
    logger.info(s"Spark UI available at: ${spark.sparkContext.uiWebUrl.getOrElse("N/A")}")

    spark
  }

  def buildLocal(appName: String = "QuickChat-ETL-Local"): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.default.parallelism", "4")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }
}
