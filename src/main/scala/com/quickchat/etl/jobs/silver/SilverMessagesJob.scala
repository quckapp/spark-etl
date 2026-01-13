package com.quickchat.etl.jobs.silver

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.transformers.CommonTransformers
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Silver Layer ETL Job for Messages
 *
 * Transforms Bronze messages into cleaned, enriched Silver layer:
 * - Deduplication
 * - Data quality checks
 * - Enrichment with derived fields
 * - Joining with user data for denormalization
 */
object SilverMessagesJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Silver Messages ETL Job")
      run(config)
      logger.info("Silver Messages ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Silver Messages ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    // Read Bronze data
    val bronzeMessages = readBronzeMessages(config)
    val bronzeUsers = readBronzeUsers(config)

    logger.info(s"Read ${bronzeMessages.count()} Bronze messages")

    // Transform to Silver
    val silverMessages = transformToSilver(bronzeMessages, bronzeUsers, config)

    // Write to Silver layer
    writeSilverMessages(silverMessages, config)
  }

  def readBronzeMessages(config: AppConfig)(implicit spark: SparkSession): DataFrame = {
    DeltaLakeUtils.readDelta(spark, config.dataLake.bronze.messages)
  }

  def readBronzeUsers(config: AppConfig)(implicit spark: SparkSession): DataFrame = {
    DeltaLakeUtils.readDelta(spark, config.dataLake.bronze.users)
      .select(
        col("_id").as("userId"),
        col("displayName").as("senderName")
      )
  }

  def transformToSilver(
      messages: DataFrame,
      users: DataFrame,
      config: AppConfig
  ): DataFrame = {

    // Step 1: Deduplicate messages
    val dedupedMessages = CommonTransformers.deduplicate(
      messages,
      keyColumns = Seq("_id"),
      orderByColumn = "updatedAt",
      descending = true
    )

    // Step 2: Data quality - filter out invalid records
    val validMessages = dedupedMessages
      .filter(col("_id").isNotNull)
      .filter(col("conversationId").isNotNull)
      .filter(col("senderId").isNotNull)
      .filter(col("createdAt").isNotNull)

    // Step 3: Join with users for sender name
    val enrichedWithUsers = validMessages
      .join(
        users,
        validMessages("senderId") === users("userId"),
        "left"
      )
      .drop("userId")

    // Step 4: Calculate derived fields
    val enrichedMessages = enrichedWithUsers
      // Content analysis
      .withColumn("contentLength",
        when(col("content").isNotNull, length(col("content")))
          .otherwise(lit(0)))
      // Attachment analysis
      .withColumn("hasAttachments",
        size(col("attachments")) > 0)
      .withColumn("attachmentCount",
        coalesce(size(col("attachments")), lit(0)))
      .withColumn("attachmentTypes",
        when(size(col("attachments")) > 0,
          transform(col("attachments"), x => x.getField("type")))
          .otherwise(array().cast("array<string>")))
      // Reaction analysis
      .withColumn("reactionCount",
        coalesce(size(col("reactions")), lit(0)))
      .withColumn("uniqueReactors",
        when(size(col("reactions")) > 0,
          size(array_distinct(transform(col("reactions"), x => x.getField("userId")))))
          .otherwise(lit(0)))
      // Reply analysis
      .withColumn("isReply", col("replyTo").isNotNull)
      .withColumnRenamed("replyTo", "replyToMessageId")
      // Read receipt analysis
      .withColumn("readCount",
        coalesce(size(col("readBy")), lit(0)))
      // Time-based features
      .withColumn("year", year(col("createdAt")))
      .withColumn("month", month(col("createdAt")))
      .withColumn("day", dayofmonth(col("createdAt")))
      .withColumn("hour", hour(col("createdAt")))
      .withColumn("dayOfWeek", dayofweek(col("createdAt")))
      .withColumn("isWeekend", dayofweek(col("createdAt")).isin(1, 7))
      // Processing metadata
      .withColumn("processedAt", current_timestamp())

    // Step 5: Select final schema
    enrichedMessages.select(
      col("_id").as("messageId"),
      col("conversationId"),
      col("senderId"),
      col("senderName"),
      col("messageType"),
      col("content"),
      col("contentLength"),
      col("hasAttachments"),
      col("attachmentCount"),
      col("attachmentTypes"),
      col("reactionCount"),
      col("uniqueReactors"),
      col("isReply"),
      col("replyToMessageId"),
      col("isEdited"),
      col("isDeleted"),
      col("isForwarded"),
      col("readCount"),
      col("createdAt"),
      col("updatedAt"),
      col("year"),
      col("month"),
      col("day"),
      col("hour"),
      col("dayOfWeek"),
      col("isWeekend"),
      col("processedAt")
    )
  }

  def writeSilverMessages(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = config.dataLake.silver.messages

    logger.info(s"Writing Silver messages to: $targetPath")

    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("messageId"),
      partitionColumns = Seq("year", "month", "day")
    )

    // Optimize table periodically
    if (shouldOptimize()) {
      DeltaLakeUtils.optimizeTable(
        df.sparkSession,
        targetPath,
        zOrderColumns = Seq("conversationId", "senderId")
      )
    }

    logger.info(s"Successfully wrote Silver messages")
  }

  private def shouldOptimize(): Boolean = {
    // Optimize once per day (simplified logic)
    java.time.LocalTime.now().getHour == 3
  }
}
