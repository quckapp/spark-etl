package com.quickchat.etl.jobs.bronze

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.transformers.CommonTransformers
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Bronze Layer ETL Job for Messages
 *
 * Extracts raw message data from MongoDB and writes to Bronze Delta Lake
 * with minimal transformations (only adds ingestion metadata)
 */
object BronzeMessagesJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Bronze Messages ETL Job")
      run(config)
      logger.info("Bronze Messages ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Bronze Messages ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    // Extract from MongoDB
    val rawMessages = extractFromMongoDB(config)
    logger.info(s"Extracted ${rawMessages.count()} messages from MongoDB")

    // Apply bronze transformations (minimal - just add metadata)
    val bronzeMessages = transformToBronze(rawMessages)

    // Load to Delta Lake
    loadToDelta(bronzeMessages, config)
  }

  def extractFromMongoDB(config: AppConfig)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading from MongoDB collection: ${config.mongodb.collections.messages}")

    spark.read
      .format("mongodb")
      .option("uri", config.mongodb.uri)
      .option("database", config.mongodb.database)
      .option("collection", config.mongodb.collections.messages)
      .option("partitioner", config.mongodb.readConfig.partitioner)
      .load()
  }

  def transformToBronze(df: DataFrame): DataFrame = {
    // Define schema for nested structures
    val attachmentSchema = ArrayType(StructType(Seq(
      StructField("type", StringType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("filename", StringType, nullable = true),
      StructField("size", LongType, nullable = true),
      StructField("mimeType", StringType, nullable = true),
      StructField("thumbnailUrl", StringType, nullable = true)
    )))

    val reactionSchema = ArrayType(StructType(Seq(
      StructField("userId", StringType, nullable = true),
      StructField("emoji", StringType, nullable = true),
      StructField("createdAt", TimestampType, nullable = true)
    )))

    df
      // Rename MongoDB _id to string
      .withColumn("_id", col("_id.oid").cast(StringType))
      // Ensure consistent column names
      .withColumnRenamed("type", "messageType")
      // Handle nested arrays
      .withColumn("attachments",
        when(col("attachments").isNotNull, col("attachments"))
          .otherwise(array().cast(attachmentSchema)))
      .withColumn("reactions",
        when(col("reactions").isNotNull, col("reactions"))
          .otherwise(array().cast(reactionSchema)))
      .withColumn("readBy",
        when(col("readBy").isNotNull, col("readBy"))
          .otherwise(array().cast(ArrayType(StringType))))
      // Add ingestion metadata
      .transform(CommonTransformers.addIngestionMetadata(_, "mongodb"))
      // Add time partitions for efficient querying
      .transform(CommonTransformers.addTimePartitions(_, "createdAt"))
      // Select final columns
      .select(
        col("_id"),
        col("conversationId"),
        col("senderId"),
        col("messageType"),
        col("content"),
        col("attachments"),
        col("reactions"),
        col("replyTo"),
        coalesce(col("isEdited"), lit(false)).as("isEdited"),
        coalesce(col("isDeleted"), lit(false)).as("isDeleted"),
        coalesce(col("isForwarded"), lit(false)).as("isForwarded"),
        col("readBy"),
        col("createdAt"),
        col("updatedAt"),
        col("expiresAt"),
        col("ingestTimestamp"),
        col("sourceSystem"),
        col("year"),
        col("month"),
        col("day")
      )
  }

  def loadToDelta(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = config.dataLake.bronze.messages
    val partitionColumns = Seq("year", "month", "day")

    logger.info(s"Writing Bronze messages to: $targetPath")

    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("_id"),
      partitionColumns = partitionColumns
    )

    logger.info(s"Successfully wrote Bronze messages to Delta Lake")
  }
}
