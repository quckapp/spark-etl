package com.quickchat.etl.transformers

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CommonTransformers {

  /**
   * Add time-based partition columns
   */
  def addTimePartitions(df: DataFrame, timestampColumn: String): DataFrame = {
    df.withColumn("year", year(col(timestampColumn)))
      .withColumn("month", month(col(timestampColumn)))
      .withColumn("day", dayofmonth(col(timestampColumn)))
      .withColumn("hour", hour(col(timestampColumn)))
      .withColumn("dayOfWeek", dayofweek(col(timestampColumn)))
      .withColumn("isWeekend", dayofweek(col(timestampColumn)).isin(1, 7))
  }

  /**
   * Add processing metadata
   */
  def addProcessingMetadata(df: DataFrame): DataFrame = {
    df.withColumn("processedAt", current_timestamp())
      .withColumn("etlVersion", lit("1.0.0"))
  }

  /**
   * Add ingestion metadata for bronze layer
   */
  def addIngestionMetadata(df: DataFrame, sourceSystem: String): DataFrame = {
    df.withColumn("ingestTimestamp", current_timestamp())
      .withColumn("sourceSystem", lit(sourceSystem))
  }

  /**
   * Deduplicate DataFrame by key columns
   */
  def deduplicate(
      df: DataFrame,
      keyColumns: Seq[String],
      orderByColumn: String,
      descending: Boolean = true
  ): DataFrame = {
    import df.sparkSession.implicits._

    val windowSpec = if (descending) {
      org.apache.spark.sql.expressions.Window
        .partitionBy(keyColumns.map(col): _*)
        .orderBy(col(orderByColumn).desc)
    } else {
      org.apache.spark.sql.expressions.Window
        .partitionBy(keyColumns.map(col): _*)
        .orderBy(col(orderByColumn).asc)
    }

    df.withColumn("_row_num", row_number().over(windowSpec))
      .filter(col("_row_num") === 1)
      .drop("_row_num")
  }

  /**
   * Clean string columns (trim, lowercase optional)
   */
  def cleanStringColumns(df: DataFrame, columns: Seq[String], lowercase: Boolean = false): DataFrame = {
    columns.foldLeft(df) { (acc, colName) =>
      val cleanedCol = if (lowercase) {
        lower(trim(col(colName)))
      } else {
        trim(col(colName))
      }
      acc.withColumn(colName, cleanedCol)
    }
  }

  /**
   * Handle null values with defaults
   */
  def fillNulls(df: DataFrame, defaults: Map[String, Any]): DataFrame = {
    defaults.foldLeft(df) { case (acc, (colName, defaultValue)) =>
      defaultValue match {
        case s: String => acc.withColumn(colName, coalesce(col(colName), lit(s)))
        case i: Int => acc.withColumn(colName, coalesce(col(colName), lit(i)))
        case l: Long => acc.withColumn(colName, coalesce(col(colName), lit(l)))
        case d: Double => acc.withColumn(colName, coalesce(col(colName), lit(d)))
        case b: Boolean => acc.withColumn(colName, coalesce(col(colName), lit(b)))
        case _ => acc
      }
    }
  }

  /**
   * Calculate days between two dates
   */
  def daysBetween(startCol: Column, endCol: Column): Column = {
    datediff(endCol, startCol)
  }

  /**
   * Calculate days since a date until now
   */
  def daysSince(dateCol: Column): Column = {
    datediff(current_date(), dateCol)
  }

  /**
   * Categorize numeric values into buckets
   */
  def categorize(
      valueCol: Column,
      buckets: Seq[(Double, Double, String)]
  ): Column = {
    buckets.foldRight(lit("unknown"): Column) { case ((min, max, label), acc) =>
      when(valueCol >= min && valueCol < max, lit(label)).otherwise(acc)
    }
  }

  /**
   * Duration category (for calls)
   */
  def durationCategory(durationSeconds: Column): Column = {
    when(durationSeconds < 60, lit("short"))
      .when(durationSeconds < 600, lit("medium"))
      .otherwise(lit("long"))
  }

  /**
   * User activity segment based on engagement
   */
  def userSegment(activeDaysLast30: Column, messagesSent: Column): Column = {
    when(activeDaysLast30 >= 25 && messagesSent >= 100, lit("power_user"))
      .when(activeDaysLast30 >= 15 && messagesSent >= 30, lit("regular"))
      .when(activeDaysLast30 >= 5, lit("casual"))
      .when(activeDaysLast30 >= 1, lit("at_risk"))
      .otherwise(lit("churned"))
  }

  /**
   * Churn risk based on activity trend
   */
  def churnRisk(
      activeDaysLast7: Column,
      activeDaysLast30: Column,
      daysSinceLastActivity: Column
  ): Column = {
    when(daysSinceLastActivity > 14, lit("high"))
      .when(daysSinceLastActivity > 7, lit("medium"))
      .when(activeDaysLast7 < activeDaysLast30 / 4 * 0.5, lit("medium"))
      .otherwise(lit("low"))
  }

  /**
   * Conversation health score
   */
  def conversationHealth(
      messagesLast7Days: Column,
      messagesLast30Days: Column,
      daysSinceLastMessage: Column
  ): Column = {
    when(daysSinceLastMessage > 30, lit("dead"))
      .when(daysSinceLastMessage > 14, lit("dying"))
      .when(messagesLast7Days < 3, lit("quiet"))
      .when(messagesLast7Days >= 10, lit("thriving"))
      .otherwise(lit("active"))
  }

  /**
   * Extract array size safely
   */
  def safeArraySize(arrayCol: Column): Column = {
    coalesce(size(arrayCol), lit(0))
  }

  /**
   * Flatten nested struct to columns
   */
  def flattenStruct(df: DataFrame, structColumn: String, prefix: String = ""): DataFrame = {
    val schema = df.schema(structColumn).dataType.asInstanceOf[StructType]
    val flattenedColumns = schema.fields.map { field =>
      val colName = if (prefix.nonEmpty) s"${prefix}_${field.name}" else field.name
      col(s"$structColumn.${field.name}").as(colName)
    }

    df.select(
      df.columns.filter(_ != structColumn).map(col) ++ flattenedColumns: _*
    )
  }

  /**
   * Calculate engagement score (0-100)
   */
  def engagementScore(
      messagesSent: Column,
      reactionsGiven: Column,
      callsMade: Column,
      activeDays: Column,
      maxDays: Int = 30
  ): Column = {
    val msgScore = least(messagesSent / 100.0 * 30, lit(30)) // max 30 points
    val reactionScore = least(reactionsGiven / 20.0 * 20, lit(20)) // max 20 points
    val callScore = least(callsMade / 10.0 * 20, lit(20)) // max 20 points
    val activityScore = (activeDays.cast("double") / maxDays) * 30 // max 30 points

    round(msgScore + reactionScore + callScore + activityScore, 2)
  }

  /**
   * Activity trend (comparing recent vs past)
   */
  def activityTrend(recentActivity: Column, pastActivity: Column): Column = {
    val ratio = when(pastActivity === 0, lit(1.0))
      .otherwise(recentActivity.cast("double") / pastActivity.cast("double"))

    when(ratio > 1.2, lit("increasing"))
      .when(ratio < 0.8, lit("decreasing"))
      .otherwise(lit("stable"))
  }
}
