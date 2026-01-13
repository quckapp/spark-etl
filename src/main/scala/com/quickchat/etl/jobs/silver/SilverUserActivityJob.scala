package com.quickchat.etl.jobs.silver

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Silver Layer ETL Job for User Activity
 *
 * Aggregates daily user activity metrics from messages, calls, and other sources
 * Creates a daily activity summary per user
 */
object SilverUserActivityJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Silver User Activity ETL Job")
      run(config)
      logger.info("Silver User Activity ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Silver User Activity ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    // Read Silver messages and calls
    val silverMessages = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.messages)
    val bronzeCalls = DeltaLakeUtils.readDelta(spark, config.dataLake.bronze.calls)

    // Calculate activity metrics
    val userActivity = calculateUserActivity(silverMessages, bronzeCalls)

    // Write to Silver layer
    writeUserActivity(userActivity, config)
  }

  def calculateUserActivity(
      messages: DataFrame,
      calls: DataFrame
  )(implicit spark: SparkSession): DataFrame = {

    // Message activity per user per day
    val messageActivity = messages
      .withColumn("activityDate", to_date(col("createdAt")))
      .groupBy(col("senderId").as("userId"), col("activityDate"))
      .agg(
        count("*").as("messagesSent"),
        sum(when(col("hasAttachments"), 1).otherwise(0)).as("mediaShared"),
        sum(col("reactionCount")).as("reactionsReceived"),
        countDistinct(col("conversationId")).as("conversationsActive")
      )

    // Messages received per user per day
    val messagesReceived = messages
      .withColumn("activityDate", to_date(col("createdAt")))
      // Assuming we have a way to track receivers - simplified here
      .groupBy(col("conversationId"), col("activityDate"))
      .agg(count("*").as("messagesInConv"))

    // Reactions given per user per day
    val reactionsGiven = messages
      .filter(col("reactionCount") > 0)
      .withColumn("activityDate", to_date(col("createdAt")))
      .groupBy(col("senderId").as("userId"), col("activityDate"))
      .agg(sum(col("reactionCount")).as("reactionsGiven"))

    // Call activity per user per day
    val callActivity = calls
      .withColumn("activityDate", to_date(col("createdAt")))
      .groupBy(col("initiatorId").as("userId"), col("activityDate"))
      .agg(
        count("*").as("callsInitiated"),
        sum(when(col("status") === "ended", coalesce(col("duration"), lit(0L))).otherwise(0L))
          .as("totalCallDurationSeconds")
      )

    // Calls received (participant)
    val callsReceived = calls
      .withColumn("activityDate", to_date(col("createdAt")))
      .select(
        explode(col("participantIds")).as("userId"),
        col("activityDate"),
        col("duration")
      )
      .filter(col("userId").isNotNull)
      .groupBy(col("userId"), col("activityDate"))
      .agg(count("*").as("callsReceived"))

    // New conversations started
    val newConversations = messages
      .withColumn("activityDate", to_date(col("createdAt")))
      // First message in conversation by user
      .groupBy(col("senderId").as("userId"), col("conversationId"))
      .agg(min(to_date(col("createdAt"))).as("firstMessageDate"))
      .groupBy(col("userId"), col("firstMessageDate").as("activityDate"))
      .agg(count("*").as("newConversationsStarted"))

    // Join all activity metrics
    val fullActivity = messageActivity
      .join(reactionsGiven, Seq("userId", "activityDate"), "left")
      .join(callActivity, Seq("userId", "activityDate"), "left")
      .join(callsReceived, Seq("userId", "activityDate"), "left")
      .join(newConversations, Seq("userId", "activityDate"), "left")

    // Fill nulls and calculate final metrics
    fullActivity
      .withColumn("messagesSent", coalesce(col("messagesSent"), lit(0L)))
      .withColumn("messagesReceived", lit(0L)) // Placeholder - need proper tracking
      .withColumn("reactionsGiven", coalesce(col("reactionsGiven"), lit(0L)))
      .withColumn("reactionsReceived", coalesce(col("reactionsReceived"), lit(0L)))
      .withColumn("callsInitiated", coalesce(col("callsInitiated"), lit(0L)))
      .withColumn("callsReceived", coalesce(col("callsReceived"), lit(0L)))
      .withColumn("totalCallDurationMinutes",
        coalesce(col("totalCallDurationSeconds"), lit(0L)) / 60.0)
      .withColumn("conversationsActive", coalesce(col("conversationsActive"), lit(0L)))
      .withColumn("newConversationsStarted", coalesce(col("newConversationsStarted"), lit(0L)))
      .withColumn("mediaShared", coalesce(col("mediaShared"), lit(0L)))
      .withColumn("isActiveDay",
        col("messagesSent") > 0 || col("callsInitiated") > 0 || col("callsReceived") > 0)
      .withColumn("year", year(col("activityDate")))
      .withColumn("month", month(col("activityDate")))
      .withColumn("day", dayofmonth(col("activityDate")))
      .withColumn("dayOfWeek", dayofweek(col("activityDate")))
      .withColumn("processedAt", current_timestamp())
      .select(
        col("userId"),
        col("activityDate"),
        col("messagesSent"),
        col("messagesReceived"),
        col("reactionsGiven"),
        col("reactionsReceived"),
        col("callsInitiated"),
        col("callsReceived"),
        col("totalCallDurationMinutes"),
        col("conversationsActive"),
        col("newConversationsStarted"),
        col("mediaShared"),
        col("isActiveDay"),
        col("year"),
        col("month"),
        col("day"),
        col("dayOfWeek"),
        col("processedAt")
      )
  }

  def writeUserActivity(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = config.dataLake.silver.userActivity
    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("userId", "activityDate"),
      partitionColumns = Seq("year", "month")
    )
    logger.info(s"Successfully wrote Silver user activity")
  }
}
