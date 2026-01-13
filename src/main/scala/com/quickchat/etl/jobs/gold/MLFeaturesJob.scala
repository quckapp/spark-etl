package com.quickchat.etl.jobs.gold

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Gold Layer ETL Job for ML Features
 *
 * Generates feature vectors for machine learning models:
 * - Churn prediction features
 * - User behavior features
 * - Temporal features
 */
object MLFeaturesJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting ML Features ETL Job")
      run(config)
      logger.info("ML Features ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"ML Features ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    val silverUserActivity = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.userActivity)
    val silverUsers = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.users)
    val silverMessages = DeltaLakeUtils.readDelta(spark, config.dataLake.silver.messages)

    val mlFeatures = generateMLFeatures(
      silverUserActivity,
      silverUsers,
      silverMessages
    )

    writeMLFeatures(mlFeatures, config)
  }

  def generateMLFeatures(
      userActivity: DataFrame,
      users: DataFrame,
      messages: DataFrame
  )(implicit spark: SparkSession): DataFrame = {

    val featureDate = current_date()

    // Time windows
    val last1Day = date_sub(featureDate, 1)
    val last7Days = date_sub(featureDate, 7)
    val last30Days = date_sub(featureDate, 30)
    val prev7Days = date_sub(featureDate, 14) // 7-14 days ago for trend comparison

    // Activity features for different time windows
    val activityFeatures = userActivity
      .groupBy(col("userId"))
      .agg(
        // Last 1 day
        sum(when(col("activityDate") >= last1Day, col("messagesSent")).otherwise(0L)).as("msgSentLast1d"),
        sum(when(col("activityDate") >= last1Day, col("messagesReceived")).otherwise(0L)).as("msgRecvLast1d"),
        // Last 7 days
        sum(when(col("activityDate") >= last7Days, col("messagesSent")).otherwise(0L)).as("msgSentLast7d"),
        sum(when(col("activityDate") >= last7Days, col("messagesReceived")).otherwise(0L)).as("msgRecvLast7d"),
        sum(when(col("activityDate") >= last7Days && col("isActiveDay"), 1).otherwise(0)).as("activeDaysLast7d"),
        // Last 30 days
        sum(when(col("activityDate") >= last30Days, col("messagesSent")).otherwise(0L)).as("msgSentLast30d"),
        sum(when(col("activityDate") >= last30Days, col("messagesReceived")).otherwise(0L)).as("msgRecvLast30d"),
        sum(when(col("activityDate") >= last30Days && col("isActiveDay"), 1).otherwise(0)).as("activeDaysLast30d"),
        // Average session duration (placeholder - would need session data)
        avg(when(col("activityDate") >= last30Days, col("totalCallDurationMinutes")).otherwise(0.0))
          .as("avgSessionDurationMinutes"),
        // Last active date
        max(when(col("isActiveDay"), col("activityDate"))).as("lastActiveDate")
      )

    // Social/Network features
    val socialFeatures = messages
      .filter(col("createdAt") >= last30Days)
      .groupBy(col("senderId").as("userId"))
      .agg(
        countDistinct(col("conversationId")).as("uniqueContacts"),
        // Group count would need conversation type join
        lit(0L).as("groupCount")
      )

    // Average group size (placeholder)
    val groupSizeFeatures = messages
      .groupBy(col("senderId").as("userId"))
      .agg(lit(5.0).as("avgGroupSize"))

    // Temporal features - preferred hour of activity
    val temporalFeatures = messages
      .filter(col("createdAt") >= last30Days)
      .groupBy(col("senderId").as("userId"))
      .agg(
        // Most common hour
        mode(hour(col("createdAt"))).as("preferredHour"),
        // Weekday vs weekend ratio
        (sum(when(!col("isWeekend"), 1).otherwise(0)).cast("double") /
          greatest(count("*").cast("double"), lit(1.0))).as("weekdayActivityRatio")
      )

    // Calculate message velocity (messages per day trend)
    val velocityFeatures = userActivity
      .groupBy(col("userId"))
      .agg(
        // Last 7 days messages / 7
        (sum(when(col("activityDate") >= last7Days, col("messagesSent")).otherwise(0L)).cast("double") / 7.0)
          .as("messageVelocity7d"),
        // Compare last 7 days to previous 7 days
        (sum(when(col("activityDate") >= last7Days, col("messagesSent")).otherwise(0L)).cast("double") -
          sum(when(col("activityDate") >= prev7Days && col("activityDate") < last7Days, col("messagesSent"))
            .otherwise(0L)).cast("double")).as("engagementTrend")
      )

    // Generate churn label (for supervised learning)
    // User is churned if no activity in next 7 days
    // This would be calculated retrospectively for training data
    val churnLabels = userActivity
      .groupBy(col("userId"))
      .agg(
        max(col("activityDate")).as("maxActivityDate")
      )
      .withColumn("willChurnNext7d",
        when(datediff(featureDate, col("maxActivityDate")) > 7, true)
          .otherwise(false))
      .select(col("userId"), col("willChurnNext7d"))

    // Join all features
    val allFeatures = users
      .select(col("userId"))
      .join(activityFeatures, Seq("userId"), "left")
      .join(socialFeatures, Seq("userId"), "left")
      .join(groupSizeFeatures, Seq("userId"), "left")
      .join(temporalFeatures, Seq("userId"), "left")
      .join(velocityFeatures, Seq("userId"), "left")
      .join(churnLabels, Seq("userId"), "left")

    // Fill nulls and add metadata
    allFeatures
      .na.fill(0L, Seq(
        "msgSentLast1d", "msgRecvLast1d", "msgSentLast7d", "msgRecvLast7d",
        "msgSentLast30d", "msgRecvLast30d", "activeDaysLast7d", "activeDaysLast30d",
        "uniqueContacts", "groupCount"
      ))
      .na.fill(0.0, Seq(
        "avgSessionDurationMinutes", "avgGroupSize", "weekdayActivityRatio",
        "messageVelocity7d", "engagementTrend"
      ))
      .na.fill(12, Seq("preferredHour"))
      .withColumn("featureDate", featureDate)
      .withColumn("processedAt", current_timestamp())
      .select(
        col("userId"),
        col("featureDate"),
        // Activity features
        col("msgSentLast1d"),
        col("msgSentLast7d"),
        col("msgSentLast30d"),
        col("msgRecvLast1d"),
        col("msgRecvLast7d"),
        col("msgRecvLast30d"),
        // Engagement features
        col("activeDaysLast7d"),
        col("activeDaysLast30d"),
        col("avgSessionDurationMinutes"),
        // Social features
        col("uniqueContacts"),
        col("groupCount"),
        col("avgGroupSize"),
        // Temporal features
        col("preferredHour"),
        col("weekdayActivityRatio"),
        // Derived features
        col("messageVelocity7d"),
        col("engagementTrend"),
        // Label
        col("willChurnNext7d"),
        col("processedAt")
      )
  }

  def writeMLFeatures(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = config.dataLake.gold.mlFeatures

    // Write with overwrite for feature store (latest snapshot)
    DeltaLakeUtils.writeDelta(
      df,
      targetPath,
      partitionColumns = Seq.empty,
      mode = org.apache.spark.sql.SaveMode.Overwrite
    )

    logger.info(s"Successfully wrote ML features to $targetPath")
  }
}
