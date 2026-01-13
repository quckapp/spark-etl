package com.quickchat.etl.jobs.gold

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.transformers.CommonTransformers
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Gold Layer ETL Job for User Engagement Metrics
 *
 * Calculates per-user engagement metrics:
 * - Engagement score
 * - Activity trends
 * - Churn risk
 * - User segmentation
 */
object UserEngagementJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting User Engagement ETL Job")
      run(config)
      logger.info("User Engagement ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"User Engagement ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    val silverUsers = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.users)
    val silverUserActivity = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.userActivity)
    val silverMessages = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.messages)

    val userEngagement = calculateUserEngagement(
      silverUsers,
      silverUserActivity,
      silverMessages
    )

    writeUserEngagement(userEngagement, config)
  }

  def calculateUserEngagement(
      users: DataFrame,
      userActivity: DataFrame,
      messages: DataFrame
  )(implicit spark: SparkSession): DataFrame = {

    val calculationDate = current_date()
    val last7Days = date_sub(calculationDate, 7)
    val last30Days = date_sub(calculationDate, 30)

    // Activity metrics for last 7 and 30 days
    val activityLast7Days = userActivity
      .filter(col("activityDate") >= last7Days)
      .groupBy(col("userId"))
      .agg(
        sum(col("messagesSent")).as("messagesSentLast7"),
        sum(col("messagesReceived")).as("messagesReceivedLast7"),
        sum(col("reactionsGiven")).as("reactionsGivenLast7"),
        sum(col("reactionsReceived")).as("reactionsReceivedLast7"),
        sum(col("callsInitiated")).as("callsInitiatedLast7"),
        sum(col("callsReceived")).as("callsReceivedLast7"),
        sum(col("totalCallDurationMinutes")).as("callDurationLast7"),
        sum(when(col("isActiveDay"), 1).otherwise(0)).as("activeDaysLast7")
      )

    val activityLast30Days = userActivity
      .filter(col("activityDate") >= last30Days)
      .groupBy(col("userId"))
      .agg(
        sum(col("messagesSent")).as("messagesSentLast30"),
        sum(col("messagesReceived")).as("messagesReceivedLast30"),
        sum(col("reactionsGiven")).as("reactionsGivenLast30"),
        sum(col("reactionsReceived")).as("reactionsReceivedLast30"),
        sum(col("callsInitiated")).as("callsInitiatedLast30"),
        sum(col("callsReceived")).as("callsReceivedLast30"),
        sum(col("totalCallDurationMinutes")).as("callDurationLast30"),
        sum(when(col("isActiveDay"), 1).otherwise(0)).as("activeDaysLast30"),
        countDistinct(col("activityDate")).as("uniqueActiveDaysLast30"),
        avg(col("messagesSent")).as("avgDailyMessagesLast30")
      )

    // Total lifetime metrics
    val lifetimeMetrics = userActivity
      .groupBy(col("userId"))
      .agg(
        sum(col("messagesSent")).as("totalMessagesSent"),
        sum(col("messagesReceived")).as("totalMessagesReceived"),
        sum(col("reactionsGiven")).as("totalReactionsGiven"),
        sum(col("reactionsReceived")).as("totalReactionsReceived"),
        sum(col("callsInitiated")).as("totalCallsInitiated"),
        sum(col("callsReceived")).as("totalCallsReceived"),
        sum(col("totalCallDurationMinutes")).as("totalCallDurationMinutes"),
        max(col("activityDate")).as("lastActiveDate")
      )

    // Network metrics (unique contacts)
    val networkMetrics = messages
      .groupBy(col("senderId").as("userId"))
      .agg(
        countDistinct(col("conversationId")).as("conversationsParticipating")
      )

    // Group participation
    val groupMetrics = messages
      .join(
        spark.read.format("delta").load(spark.conf.get("spark.sql.warehouse.dir") + "/bronze/conversations")
          .filter(col("conversationType") === "group")
          .select(col("_id").as("convId")),
        messages("conversationId") === col("convId"),
        "inner"
      )
      .groupBy(col("senderId").as("userId"))
      .agg(countDistinct(col("conversationId")).as("groupsParticipating"))

    // Join all metrics
    val allMetrics = users
      .select(col("userId"))
      .join(activityLast7Days, Seq("userId"), "left")
      .join(activityLast30Days, Seq("userId"), "left")
      .join(lifetimeMetrics, Seq("userId"), "left")
      .join(networkMetrics, Seq("userId"), "left")

    // Fill nulls
    val filledMetrics = allMetrics
      .na.fill(0L, Seq(
        "messagesSentLast7", "messagesReceivedLast7", "reactionsGivenLast7",
        "reactionsReceivedLast7", "callsInitiatedLast7", "callsReceivedLast7",
        "activeDaysLast7", "messagesSentLast30", "messagesReceivedLast30",
        "reactionsGivenLast30", "reactionsReceivedLast30", "callsInitiatedLast30",
        "callsReceivedLast30", "activeDaysLast30", "uniqueActiveDaysLast30",
        "totalMessagesSent", "totalMessagesReceived", "totalReactionsGiven",
        "totalReactionsReceived", "totalCallsInitiated", "totalCallsReceived",
        "conversationsParticipating"
      ))
      .na.fill(0.0, Seq("callDurationLast7", "callDurationLast30", "totalCallDurationMinutes", "avgDailyMessagesLast30"))

    // Calculate engagement score and segments
    filledMetrics
      .withColumn("calculationDate", calculationDate)
      .withColumn("daysSinceLastActive",
        when(col("lastActiveDate").isNotNull,
          datediff(calculationDate, col("lastActiveDate")))
          .otherwise(lit(999)))
      // Engagement Score (0-100)
      .withColumn("engagementScore",
        CommonTransformers.engagementScore(
          col("messagesSentLast30"),
          col("reactionsGivenLast30"),
          col("callsInitiatedLast30") + col("callsReceivedLast30"),
          col("activeDaysLast30"),
          30
        ))
      // Activity Trend
      .withColumn("activityTrend",
        CommonTransformers.activityTrend(
          col("messagesSentLast7"),
          col("messagesSentLast30") - col("messagesSentLast7")
        ))
      // Churn Risk
      .withColumn("churnRisk",
        CommonTransformers.churnRisk(
          col("activeDaysLast7"),
          col("activeDaysLast30"),
          col("daysSinceLastActive")
        ))
      // User Segment
      .withColumn("userSegment",
        CommonTransformers.userSegment(
          col("activeDaysLast30"),
          col("messagesSentLast30")
        ))
      .withColumn("year", year(col("calculationDate")))
      .withColumn("month", month(col("calculationDate")))
      .withColumn("processedAt", current_timestamp())
      .select(
        col("userId"),
        col("calculationDate"),
        col("totalMessagesSent"),
        col("totalMessagesReceived"),
        col("totalReactionsGiven"),
        col("totalReactionsReceived"),
        col("totalCallsInitiated"),
        col("totalCallsReceived"),
        col("totalCallDurationMinutes"),
        col("activeDaysLast7").as("activeDaysLast7"),
        col("activeDaysLast30").as("activeDaysLast30"),
        col("avgDailyMessagesLast30").as("avgDailyMessages"),
        lit(null).cast("double").as("avgResponseTimeMinutes"),
        col("conversationsParticipating").as("uniqueContactsMessaged"),
        col("conversationsParticipating"),
        lit(0L).as("groupsParticipating"),
        col("engagementScore"),
        col("activityTrend"),
        col("churnRisk"),
        col("userSegment"),
        col("year"),
        col("month"),
        col("processedAt")
      )
  }

  def writeUserEngagement(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = s"${config.dataLake.gold.analytics}/user_engagement"
    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("userId", "calculationDate"),
      partitionColumns = Seq("year", "month")
    )
    logger.info(s"Successfully wrote user engagement metrics")
  }
}
