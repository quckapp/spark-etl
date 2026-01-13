package com.quickchat.etl.jobs.streaming

import com.quickchat.etl.config.AppConfig
import com.quickchat.etl.transformers.CommonTransformers
import com.quickchat.etl.utils.SparkSessionBuilder
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

/**
 * Kafka Streaming ETL Job
 *
 * Real-time ingestion from Kafka topics to Bronze Delta Lake
 * Uses Structured Streaming with exactly-once semantics
 */
object KafkaStreamingJob extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    implicit val spark: SparkSession = SparkSessionBuilder.build(config)

    try {
      logger.info("Starting Kafka Streaming ETL Job")

      // Start multiple streams for different topics
      val messageStream = startMessageStream(config)
      val eventStream = startEventStream(config)

      // Await termination
      spark.streams.awaitAnyTermination()
    } catch {
      case e: Exception =>
        logger.error(s"Kafka Streaming ETL Job failed: ${e.getMessage}", e)
        throw e
    }
  }

  private val messageSchema = StructType(Seq(
    StructField("_id", StringType),
    StructField("conversationId", StringType),
    StructField("senderId", StringType),
    StructField("type", StringType),
    StructField("content", StringType),
    StructField("attachments", ArrayType(StructType(Seq(
      StructField("type", StringType),
      StructField("url", StringType),
      StructField("filename", StringType)
    )))),
    StructField("reactions", ArrayType(StructType(Seq(
      StructField("userId", StringType),
      StructField("emoji", StringType)
    )))),
    StructField("replyTo", StringType),
    StructField("isEdited", BooleanType),
    StructField("isDeleted", BooleanType),
    StructField("isForwarded", BooleanType),
    StructField("createdAt", TimestampType),
    StructField("updatedAt", TimestampType)
  ))

  private val eventSchema = StructType(Seq(
    StructField("eventId", StringType),
    StructField("eventType", StringType),
    StructField("entityType", StringType),
    StructField("entityId", StringType),
    StructField("userId", StringType),
    StructField("payload", StringType),
    StructField("timestamp", TimestampType)
  ))

  def startMessageStream(config: AppConfig)(implicit spark: SparkSession): Unit = {
    logger.info(s"Starting message stream from topic: ${config.kafka.topics.messages}")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", config.kafka.topics.messages)
      .option("startingOffsets", config.kafka.consumer.autoOffsetReset)
      .option("failOnDataLoss", "false")
      .load()

    val messagesDF = kafkaDF
      .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
      .select(
        from_json(col("json_value"), messageSchema).as("data"),
        col("kafka_timestamp")
      )
      .select("data.*", "kafka_timestamp")
      .withColumnRenamed("type", "messageType")
      .withColumn("ingestTimestamp", col("kafka_timestamp"))
      .withColumn("sourceSystem", lit("kafka"))
      .transform(CommonTransformers.addTimePartitions(_, "createdAt"))
      // Add watermark for late data handling
      .withWatermark("createdAt", config.etl.streaming.watermarkDelay)

    val query = messagesDF.writeStream
      .format("delta")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"${config.kafka.streaming.checkpointLocation}/messages")
      .partitionBy("year", "month", "day")
      .trigger(Trigger.ProcessingTime(config.etl.streaming.triggerInterval))
      .start(config.dataLake.bronze.messages)

    logger.info(s"Message stream started with query ID: ${query.id}")
  }

  def startEventStream(config: AppConfig)(implicit spark: SparkSession): Unit = {
    logger.info(s"Starting event stream from topic: ${config.kafka.topics.events}")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", config.kafka.topics.events)
      .option("startingOffsets", config.kafka.consumer.autoOffsetReset)
      .option("failOnDataLoss", "false")
      .load()

    val eventsDF = kafkaDF
      .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
      .select(
        from_json(col("json_value"), eventSchema).as("data"),
        col("kafka_timestamp")
      )
      .select("data.*", "kafka_timestamp")
      .withColumn("ingestTimestamp", col("kafka_timestamp"))
      .withColumn("sourceSystem", lit("kafka"))
      .transform(CommonTransformers.addTimePartitions(_, "timestamp"))
      .withWatermark("timestamp", config.etl.streaming.watermarkDelay)

    val query = eventsDF.writeStream
      .format("delta")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", s"${config.kafka.streaming.checkpointLocation}/events")
      .partitionBy("year", "month", "day")
      .trigger(Trigger.ProcessingTime(config.etl.streaming.triggerInterval))
      .start(config.dataLake.bronze.events)

    logger.info(s"Event stream started with query ID: ${query.id}")
  }

  /**
   * Real-time aggregation for live dashboard metrics
   */
  def startLiveMetricsStream(config: AppConfig)(implicit spark: SparkSession): Unit = {
    logger.info("Starting live metrics aggregation stream")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", config.kafka.topics.messages)
      .option("startingOffsets", "latest")
      .load()

    val messagesDF = kafkaDF
      .selectExpr("CAST(value AS STRING) as json_value", "timestamp")
      .select(
        from_json(col("json_value"), messageSchema).as("data"),
        col("timestamp")
      )
      .select("data.*", "timestamp")
      .withWatermark("timestamp", "10 minutes")

    // Aggregate metrics per minute
    val metricsDF = messagesDF
      .groupBy(window(col("timestamp"), "1 minute"))
      .agg(
        count("*").as("messageCount"),
        countDistinct(col("senderId")).as("activeUsers"),
        countDistinct(col("conversationId")).as("activeConversations"),
        avg(length(col("content"))).as("avgMessageLength")
      )
      .select(
        col("window.start").as("windowStart"),
        col("window.end").as("windowEnd"),
        col("messageCount"),
        col("activeUsers"),
        col("activeConversations"),
        col("avgMessageLength")
      )

    // Write to console for debugging or to another Kafka topic for dashboard
    val query = metricsDF.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    logger.info(s"Live metrics stream started with query ID: ${query.id}")
  }
}
