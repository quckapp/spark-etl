package com.quickchat.etl.jobs.gold

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Gold Layer ETL Job for Daily Metrics
 *
 * Aggregates daily platform-wide metrics:
 * - DAU (Daily Active Users)
 * - Message counts
 * - Call statistics
 * - Engagement metrics
 */
object DailyMetricsJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Daily Metrics ETL Job")
      run(config)
      logger.info("Daily Metrics ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Daily Metrics ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    val silverMessages = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.messages)
    val silverUsers = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.users)
    val silverUserActivity = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.userActivity)
    val bronzeCalls = DeltaLakeUtils.readDelta(spark, config.dataLake.bronze.calls)
    val bronzeConversations = DeltaLakeUtils.readDelta(spark, config.dataLake.bronze.conversations)

    val dailyMetrics = calculateDailyMetrics(
      silverMessages,
      silverUsers,
      silverUserActivity,
      bronzeCalls,
      bronzeConversations
    )

    writeDailyMetrics(dailyMetrics, config)
  }

  def calculateDailyMetrics(
      messages: DataFrame,
      users: DataFrame,
      userActivity: DataFrame,
      calls: DataFrame,
      conversations: DataFrame
  )(implicit spark: SparkSession): DataFrame = {

    // User metrics per day
    val userMetrics = users
      .withColumn("metricDate", to_date(col("createdAt")))
      .groupBy(col("metricDate"))
      .agg(
        count("*").as("newUsers")
      )

    // Total users up to each date
    val totalUsersPerDay = users
      .withColumn("createdDate", to_date(col("createdAt")))
      .select(col("userId"), col("createdDate"))
      .distinct()

    // Active users per day (DAU)
    val dauMetrics = userActivity
      .filter(col("isActiveDay"))
      .groupBy(col("activityDate").as("metricDate"))
      .agg(
        countDistinct(col("userId")).as("activeUsers"),
        sum(col("messagesSent")).as("totalMessagesSent"),
        avg(col("messagesSent")).as("avgMessagesPerUser")
      )

    // Message metrics per day
    val messageMetrics = messages
      .withColumn("metricDate", to_date(col("createdAt")))
      .groupBy(col("metricDate"))
      .agg(
        count("*").as("totalMessages"),
        sum(when(col("messageType") === "text", 1).otherwise(0)).as("textMessages"),
        sum(when(col("messageType").isin("image", "video", "audio", "file"), 1).otherwise(0))
          .as("mediaMessages"),
        sum(col("reactionCount")).as("totalReactions"),
        // Peak hour calculation
        max(struct(
          count("*").over(org.apache.spark.sql.expressions.Window.partitionBy(
            to_date(col("createdAt")), hour(col("createdAt"))
          )),
          hour(col("createdAt"))
        )).getField("col2").as("peakHour")
      )

    // Call metrics per day
    val callMetrics = calls
      .withColumn("metricDate", to_date(col("createdAt")))
      .groupBy(col("metricDate"))
      .agg(
        count("*").as("totalCalls"),
        sum(when(col("callType") === "audio", 1).otherwise(0)).as("audioCalls"),
        sum(when(col("callType") === "video", 1).otherwise(0)).as("videoCalls"),
        sum(coalesce(col("duration"), lit(0L))).as("totalCallDurationSeconds"),
        avg(coalesce(col("duration"), lit(0L))).as("avgCallDurationSeconds")
      )
      .withColumn("totalCallDurationMinutes", col("totalCallDurationSeconds") / 60.0)
      .withColumn("avgCallDurationMinutes", col("avgCallDurationSeconds") / 60.0)
      .drop("totalCallDurationSeconds", "avgCallDurationSeconds")

    // Conversation metrics per day
    val conversationMetrics = conversations
      .withColumn("metricDate", to_date(col("createdAt")))
      .groupBy(col("metricDate"))
      .agg(
        count("*").as("newConversations")
      )

    // Active conversations (had message in that day)
    val activeConversations = messages
      .withColumn("metricDate", to_date(col("createdAt")))
      .groupBy(col("metricDate"))
      .agg(
        countDistinct(col("conversationId")).as("activeConversations")
      )

    // Join all metrics
    val allMetrics = dauMetrics
      .join(userMetrics, Seq("metricDate"), "left")
      .join(messageMetrics, Seq("metricDate"), "left")
      .join(callMetrics, Seq("metricDate"), "left")
      .join(conversationMetrics, Seq("metricDate"), "left")
      .join(activeConversations, Seq("metricDate"), "left")

    // Calculate cumulative total users
    val windowSpec = org.apache.spark.sql.expressions.Window
      .orderBy(col("metricDate"))
      .rowsBetween(Long.MinValue, 0)

    allMetrics
      .withColumn("totalUsers", sum(coalesce(col("newUsers"), lit(0L))).over(windowSpec))
      .withColumn("totalConversations", sum(coalesce(col("newConversations"), lit(0L))).over(windowSpec))
      // Fill nulls with 0
      .na.fill(0, Seq(
        "activeUsers", "newUsers", "totalMessages", "textMessages", "mediaMessages",
        "totalCalls", "audioCalls", "videoCalls", "totalReactions",
        "newConversations", "activeConversations"
      ))
      .na.fill(0.0, Seq("totalCallDurationMinutes", "avgCallDurationMinutes", "avgMessagesPerUser"))
      // Add time dimensions
      .withColumn("year", year(col("metricDate")))
      .withColumn("month", month(col("metricDate")))
      .withColumn("day", dayofmonth(col("metricDate")))
      .withColumn("dayOfWeek", dayofweek(col("metricDate")))
      .withColumn("isWeekend", dayofweek(col("metricDate")).isin(1, 7))
      .withColumn("processedAt", current_timestamp())
      // Average sessions placeholder
      .withColumn("avgSessionsPerUser", lit(1.0))
      .select(
        col("metricDate"),
        col("totalUsers"),
        col("activeUsers"),
        col("newUsers"),
        col("totalMessages"),
        col("textMessages"),
        col("mediaMessages"),
        col("totalCalls"),
        col("audioCalls"),
        col("videoCalls"),
        col("totalCallDurationMinutes"),
        col("avgCallDurationMinutes"),
        col("totalConversations"),
        col("activeConversations"),
        col("newConversations"),
        col("totalReactions"),
        col("avgMessagesPerUser"),
        col("avgSessionsPerUser"),
        coalesce(col("peakHour"), lit(12)).as("peakHour"),
        col("year"),
        col("month"),
        col("day"),
        col("dayOfWeek"),
        col("isWeekend"),
        col("processedAt")
      )
  }

  def writeDailyMetrics(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = s"${config.dataLake.gold.metrics}/daily"
    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("metricDate"),
      partitionColumns = Seq("year", "month")
    )
    logger.info(s"Successfully wrote daily metrics to Gold layer")
  }
}
