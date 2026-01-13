package com.quickchat.etl.jobs.silver

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.transformers.CommonTransformers
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Silver Layer ETL Job for Users
 *
 * Transforms Bronze users into cleaned, enriched Silver layer
 */
object SilverUsersJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Silver Users ETL Job")
      run(config)
      logger.info("Silver Users ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Silver Users ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    val bronzeUsers = DeltaLakeUtils.readDelta(spark, config.dataLake.bronze.users)
    logger.info(s"Read ${bronzeUsers.count()} Bronze users")

    val silverUsers = transformToSilver(bronzeUsers)
    writeSilverUsers(silverUsers, config)
  }

  def transformToSilver(df: DataFrame): DataFrame = {
    // Deduplicate
    val deduped = CommonTransformers.deduplicate(
      df,
      keyColumns = Seq("_id"),
      orderByColumn = "updatedAt",
      descending = true
    )

    // Data quality filtering
    val valid = deduped
      .filter(col("_id").isNotNull)
      .filter(col("createdAt").isNotNull)

    // Enrich with derived fields
    valid
      .withColumn("hasAvatar", col("avatar").isNotNull && col("avatar") =!= "")
      .withColumn("hasBio", col("bio").isNotNull && col("bio") =!= "")
      .withColumn("lastSeenDaysAgo",
        when(col("lastSeen").isNotNull,
          datediff(current_date(), col("lastSeen")))
          .otherwise(lit(null)))
      .withColumn("accountAgeDays",
        datediff(current_date(), col("createdAt")))
      .withColumn("year", year(col("createdAt")))
      .withColumn("month", month(col("createdAt")))
      .withColumn("processedAt", current_timestamp())
      .select(
        col("_id").as("userId"),
        col("phoneNumber"),
        col("email"),
        col("username"),
        col("displayName"),
        col("hasAvatar"),
        col("hasBio"),
        col("status"),
        col("lastSeen"),
        col("lastSeenDaysAgo"),
        col("isOnline"),
        col("role"),
        col("isBanned"),
        col("isVerified"),
        col("accountAgeDays"),
        col("createdAt"),
        col("updatedAt"),
        col("year"),
        col("month"),
        col("processedAt")
      )
  }

  def writeSilverUsers(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = config.dataLake.silver.users
    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("userId"),
      partitionColumns = Seq("year", "month")
    )
    logger.info(s"Successfully wrote Silver users")
  }
}
