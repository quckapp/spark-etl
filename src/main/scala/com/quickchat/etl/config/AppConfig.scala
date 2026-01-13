package com.quickchat.etl.config

import pureconfig._
import pureconfig.generic.auto._

case class AppConfig(
    app: AppSettings,
    spark: SparkConfig,
    dataLake: DataLakeConfig,
    mongodb: MongoDbConfig,
    mysql: MySqlConfig,
    kafka: KafkaConfig,
    aws: AwsConfig,
    etl: EtlConfig,
    clickhouse: ClickHouseConfig
)

case class AppSettings(
    name: String,
    environment: String
)

case class SparkConfig(
    master: String,
    appName: String,
    config: Map[String, String]
)

case class DataLakeConfig(
    basePath: String,
    bronze: BronzeLayerConfig,
    silver: SilverLayerConfig,
    gold: GoldLayerConfig,
    checkpoints: String
)

case class BronzeLayerConfig(
    path: String,
    messages: String,
    users: String,
    conversations: String,
    calls: String,
    events: String,
    notifications: String,
    media: String
)

case class SilverLayerConfig(
    path: String,
    messages: String,
    users: String,
    conversations: String,
    calls: String,
    sessions: String,
    userActivity: String
)

case class GoldLayerConfig(
    path: String,
    analytics: String,
    metrics: String,
    reports: String,
    mlFeatures: String,
    aggregations: String
)

case class MongoDbConfig(
    uri: String,
    database: String,
    collections: MongoCollections,
    readConfig: MongoReadConfig
)

case class MongoCollections(
    messages: String,
    users: String,
    conversations: String,
    calls: String,
    notifications: String
)

case class MongoReadConfig(
    partitioner: String,
    partitionerOptions: Map[String, String]
)

case class MySqlConfig(
    url: String,
    user: String,
    password: String,
    driver: String,
    tables: MySqlTables
)

case class MySqlTables(
    users: String,
    auth: String,
    sessions: String,
    auditLogs: String
)

case class KafkaConfig(
    bootstrapServers: String,
    topics: KafkaTopics,
    consumer: KafkaConsumerConfig,
    streaming: KafkaStreamingConfig
)

case class KafkaTopics(
    messages: String,
    users: String,
    calls: String,
    events: String,
    notifications: String,
    analytics: String
)

case class KafkaConsumerConfig(
    groupId: String,
    autoOffsetReset: String,
    enableAutoCommit: String
)

case class KafkaStreamingConfig(
    triggerInterval: String,
    checkpointLocation: String
)

case class AwsConfig(
    accessKey: String,
    secretKey: String,
    region: String,
    s3: S3Config
)

case class S3Config(
    endpoint: String,
    pathStyleAccess: Boolean
)

case class EtlConfig(
    batchSize: Int,
    partitionColumns: Seq[String],
    bronze: BronzeEtlConfig,
    silver: SilverEtlConfig,
    gold: GoldEtlConfig,
    streaming: StreamingEtlConfig
)

case class BronzeEtlConfig(
    retentionDays: Int,
    lookbackHours: Int
)

case class SilverEtlConfig(
    retentionDays: Int,
    dedupWindowHours: Int
)

case class GoldEtlConfig(
    retentionDays: Int,
    metricsWindowDays: Int
)

case class StreamingEtlConfig(
    triggerInterval: String,
    watermarkDelay: String
)

case class ClickHouseConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String
)

object AppConfig {
  def load(): AppConfig = {
    ConfigSource.default.at("quickchat").loadOrThrow[AppConfig]
  }
}
