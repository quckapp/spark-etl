package com.quickchat.etl.utils

import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object DeltaLakeUtils extends LazyLogging {

  /**
   * Write DataFrame to Delta Lake with partitioning
   */
  def writeDelta(
      df: DataFrame,
      path: String,
      partitionColumns: Seq[String] = Seq.empty,
      mode: SaveMode = SaveMode.Append
  ): Unit = {
    logger.info(s"Writing Delta table to: $path with mode: $mode")

    val writer = df.write
      .format("delta")
      .mode(mode)

    if (partitionColumns.nonEmpty) {
      writer.partitionBy(partitionColumns: _*)
    }

    writer.save(path)
    logger.info(s"Successfully wrote ${df.count()} records to $path")
  }

  /**
   * Upsert (merge) data into Delta table
   */
  def upsertDelta(
      spark: SparkSession,
      sourceDF: DataFrame,
      targetPath: String,
      mergeKeys: Seq[String],
      partitionColumns: Seq[String] = Seq.empty
  ): Unit = {
    logger.info(s"Upserting data to Delta table: $targetPath")

    if (!DeltaTable.isDeltaTable(spark, targetPath)) {
      logger.info(s"Delta table doesn't exist. Creating new table at $targetPath")
      writeDelta(sourceDF, targetPath, partitionColumns, SaveMode.Overwrite)
      return
    }

    val deltaTable = DeltaTable.forPath(spark, targetPath)
    val mergeCondition = mergeKeys.map(k => s"target.$k = source.$k").mkString(" AND ")

    deltaTable.as("target")
      .merge(sourceDF.as("source"), mergeCondition)
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info(s"Successfully upserted data to $targetPath")
  }

  /**
   * Read Delta table
   */
  def readDelta(spark: SparkSession, path: String): DataFrame = {
    logger.info(s"Reading Delta table from: $path")
    spark.read.format("delta").load(path)
  }

  /**
   * Read Delta table with time travel (version)
   */
  def readDeltaVersion(spark: SparkSession, path: String, version: Long): DataFrame = {
    logger.info(s"Reading Delta table from: $path at version: $version")
    spark.read.format("delta").option("versionAsOf", version).load(path)
  }

  /**
   * Read Delta table with time travel (timestamp)
   */
  def readDeltaTimestamp(spark: SparkSession, path: String, timestamp: String): DataFrame = {
    logger.info(s"Reading Delta table from: $path at timestamp: $timestamp")
    spark.read.format("delta").option("timestampAsOf", timestamp).load(path)
  }

  /**
   * Optimize Delta table (compaction)
   */
  def optimizeTable(spark: SparkSession, path: String, zOrderColumns: Seq[String] = Seq.empty): Unit = {
    logger.info(s"Optimizing Delta table: $path")

    val deltaTable = DeltaTable.forPath(spark, path)

    if (zOrderColumns.nonEmpty) {
      logger.info(s"Z-Ordering by columns: ${zOrderColumns.mkString(", ")}")
      deltaTable.optimize().executeZOrderBy(zOrderColumns: _*)
    } else {
      deltaTable.optimize().executeCompaction()
    }

    logger.info(s"Optimization completed for $path")
  }

  /**
   * Vacuum Delta table (remove old files)
   */
  def vacuumTable(spark: SparkSession, path: String, retentionHours: Double = 168): Unit = {
    logger.info(s"Vacuuming Delta table: $path with retention: $retentionHours hours")

    val deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.vacuum(retentionHours)

    logger.info(s"Vacuum completed for $path")
  }

  /**
   * Get Delta table history
   */
  def getHistory(spark: SparkSession, path: String, limit: Int = 10): DataFrame = {
    val deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.history(limit)
  }

  /**
   * Check if path is a Delta table
   */
  def isDeltaTable(spark: SparkSession, path: String): Boolean = {
    DeltaTable.isDeltaTable(spark, path)
  }

  /**
   * Create or replace Delta table from DataFrame
   */
  def createOrReplaceTable(
      df: DataFrame,
      path: String,
      partitionColumns: Seq[String] = Seq.empty
  ): Unit = {
    writeDelta(df, path, partitionColumns, SaveMode.Overwrite)
  }

  /**
   * Incremental load with watermark
   */
  def incrementalLoad(
      spark: SparkSession,
      sourceDF: DataFrame,
      targetPath: String,
      timestampColumn: String,
      lookbackHours: Int = 24
  ): DataFrame = {
    val cutoffTime = current_timestamp().minus(expr(s"INTERVAL $lookbackHours HOURS"))

    if (isDeltaTable(spark, targetPath)) {
      val lastProcessedTime = readDelta(spark, targetPath)
        .agg(max(col(timestampColumn)))
        .first()
        .getTimestamp(0)

      if (lastProcessedTime != null) {
        logger.info(s"Incremental load from: $lastProcessedTime")
        sourceDF.filter(col(timestampColumn) > lastProcessedTime)
      } else {
        logger.info(s"No previous data found. Loading all with lookback: $lookbackHours hours")
        sourceDF.filter(col(timestampColumn) >= cutoffTime)
      }
    } else {
      logger.info(s"Target table doesn't exist. Loading all with lookback: $lookbackHours hours")
      sourceDF.filter(col(timestampColumn) >= cutoffTime)
    }
  }
}
