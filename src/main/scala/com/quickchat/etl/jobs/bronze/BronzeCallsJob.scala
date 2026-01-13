package com.quickchat.etl.jobs.bronze

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.transformers.CommonTransformers
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Bronze Layer ETL Job for Calls
 */
object BronzeCallsJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Bronze Calls ETL Job")
      run(config)
      logger.info("Bronze Calls ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Bronze Calls ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    val rawCalls = extractFromMongoDB(config)
    logger.info(s"Extracted ${rawCalls.count()} calls from MongoDB")

    val bronzeCalls = transformToBronze(rawCalls)
    loadToDelta(bronzeCalls, config)
  }

  def extractFromMongoDB(config: AppConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("mongodb")
      .option("uri", config.mongodb.uri)
      .option("database", config.mongodb.database)
      .option("collection", config.mongodb.collections.calls)
      .load()
  }

  def transformToBronze(df: DataFrame): DataFrame = {
    df
      .withColumn("_id", col("_id.oid").cast(StringType))
      .withColumnRenamed("type", "callType")
      .withColumn("participantIds",
        when(col("participantIds").isNotNull, col("participantIds"))
          .otherwise(array().cast(ArrayType(StringType))))
      .transform(CommonTransformers.addIngestionMetadata(_, "mongodb"))
      .transform(CommonTransformers.addTimePartitions(_, "createdAt"))
      .select(
        col("_id"),
        col("initiatorId"),
        col("participantIds"),
        col("conversationId"),
        col("callType"),
        col("status"),
        col("startTime"),
        col("endTime"),
        col("duration"),
        col("createdAt"),
        col("ingestTimestamp"),
        col("sourceSystem"),
        col("year"),
        col("month"),
        col("day")
      )
  }

  def loadToDelta(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = config.dataLake.bronze.calls
    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("_id"),
      partitionColumns = Seq("year", "month", "day")
    )
    logger.info(s"Successfully wrote Bronze calls to Delta Lake")
  }
}
