package com.quickchat.etl.jobs.bronze

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.transformers.CommonTransformers
import com.quickchat.etl.utils.{DeltaLakeUtils, SparkSessionBuilder}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Bronze Layer ETL Job for Users
 *
 * Extracts user data from both MongoDB and MySQL (auth service)
 * and consolidates into Bronze Delta Lake
 */
object BronzeUsersJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Bronze Users ETL Job")
      run(config)
      logger.info("Bronze Users ETL Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Bronze Users ETL Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }

  def run(config: AppConfig)(implicit spark: SparkSession): Unit = {
    // Extract from MongoDB (main user data)
    val mongoUsers = extractFromMongoDB(config)
    logger.info(s"Extracted ${mongoUsers.count()} users from MongoDB")

    // Extract from MySQL (auth service data) - optional join
    val mysqlUsers = extractFromMySQL(config)
    logger.info(s"Extracted ${mysqlUsers.count()} users from MySQL")

    // Merge and transform
    val bronzeUsers = transformToBronze(mongoUsers, mysqlUsers)

    // Load to Delta Lake
    loadToDelta(bronzeUsers, config)
  }

  def extractFromMongoDB(config: AppConfig)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading from MongoDB collection: ${config.mongodb.collections.users}")

    spark.read
      .format("mongodb")
      .option("uri", config.mongodb.uri)
      .option("database", config.mongodb.database)
      .option("collection", config.mongodb.collections.users)
      .load()
  }

  def extractFromMySQL(config: AppConfig)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading from MySQL table: ${config.mysql.tables.users}")

    spark.read
      .format("jdbc")
      .option("url", config.mysql.url)
      .option("dbtable", config.mysql.tables.users)
      .option("user", config.mysql.user)
      .option("password", config.mysql.password)
      .option("driver", config.mysql.driver)
      .load()
      .select(
        col("id").as("mysql_user_id"),
        col("email").as("mysql_email"),
        col("email_verified").as("emailVerified"),
        col("two_factor_enabled").as("twoFactorEnabled"),
        col("last_login_at").as("lastLoginAt"),
        col("login_count").as("loginCount"),
        col("created_at").as("mysqlCreatedAt")
      )
  }

  def transformToBronze(mongoDF: DataFrame, mysqlDF: DataFrame): DataFrame = {
    // Transform MongoDB data
    val transformedMongo = mongoDF
      .withColumn("_id", col("_id.oid").cast(StringType))
      .select(
        col("_id"),
        col("phoneNumber"),
        col("email"),
        col("username"),
        col("displayName"),
        col("avatar"),
        col("bio"),
        col("status"),
        col("lastSeen"),
        coalesce(col("isOnline"), lit(false)).as("isOnline"),
        coalesce(col("role"), lit("user")).as("role"),
        coalesce(col("isBanned"), lit(false)).as("isBanned"),
        coalesce(col("isVerified"), lit(false)).as("isVerified"),
        col("createdAt"),
        col("updatedAt")
      )

    // Left join with MySQL auth data
    val joined = transformedMongo
      .join(
        mysqlDF,
        transformedMongo("email") === mysqlDF("mysql_email"),
        "left"
      )
      .drop("mysql_user_id", "mysql_email")

    // Add metadata and partitions
    joined
      .transform(CommonTransformers.addIngestionMetadata(_, "mongodb+mysql"))
      .transform(CommonTransformers.addTimePartitions(_, "createdAt"))
      .select(
        col("_id"),
        col("phoneNumber"),
        col("email"),
        col("username"),
        col("displayName"),
        col("avatar"),
        col("bio"),
        col("status"),
        col("lastSeen"),
        col("isOnline"),
        col("role"),
        col("isBanned"),
        col("isVerified"),
        col("emailVerified"),
        col("twoFactorEnabled"),
        col("lastLoginAt"),
        col("loginCount"),
        col("createdAt"),
        col("updatedAt"),
        col("ingestTimestamp"),
        col("sourceSystem"),
        col("year"),
        col("month")
      )
  }

  def loadToDelta(df: DataFrame, config: AppConfig): Unit = {
    val targetPath = config.dataLake.bronze.users
    val partitionColumns = Seq("year", "month")

    logger.info(s"Writing Bronze users to: $targetPath")

    DeltaLakeUtils.upsertDelta(
      df.sparkSession,
      df,
      targetPath,
      mergeKeys = Seq("_id"),
      partitionColumns = partitionColumns
    )

    logger.info(s"Successfully wrote Bronze users to Delta Lake")
  }
}
