package com.quickchat.etl.jobs.bronze

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.transformers.CommonTransformers
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Bronze Layer ETL Job for Conversations
 */
object BronzeConversationsJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Bronze Conversations ETL Job")
      run(config)
      logger.info("Bronze Conversations ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Bronze Conversations ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    val rawConversations = extractFromMongoDB(config)
    logger.info(s"Extracted ${rawConversations.count()} conversations from MongoDB")

    val bronzeConversations = transformToBronze(rawConversations)
    loadToDelta(bronzeConversations, config)
  }

  def extractFromMongoDB(config: AppConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("mongodb")
      .option("uri", config.mongodb.uri)
      .option("database", config.mongodb.database)
      .option("collection", config.mongodb.collections.conversations)
      .load()
  }

  def transformToBronze(df: DataFrame): DataFrame = {
    df
      .withColumn("_id", col("_id.oid").cast(StringType))
      .withColumnRenamed("type", "conversationType")
      .withColumn("participants",
        when(col("participants").isNotNull, col("participants"))
          .otherwise(array().cast(ArrayType(StringType))))
      .withColumn("admins",
        when(col("admins").isNotNull, col("admins"))
          .otherwise(array().cast(ArrayType(StringType))))
      .transform(CommonTransformers.addIngestionMetadata(_, "mongodb"))
      .transform(CommonTransformers.addTimePartitions(_, "createdAt"))
      .select(
        col("_id"),
        col("conversationType"),
        col("name"),
        col("avatar"),
        col("participants"),
        col("admins"),
        col("lastMessageId"),
        col("lastMessageAt"),
        coalesce(col("isArchived"), lit(false)).as("isArchived"),
        coalesce(col("isLocked"), lit(false)).as("isLocked"),
        col("createdBy"),
        col("createdAt"),
        col("updatedAt"),
        col("ingestTimestamp"),
        col("sourceSystem"),
        col("year"),
        col("month")
      )
  }

  def loadToDelta(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = config.dataLake.bronze.conversations
    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("_id"),
      partitionColumns = Seq("year", "month")
    )
    logger.info(s"Successfully wrote Bronze conversations to Delta Lake")
  }
}
