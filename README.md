# QuickChat Spark ETL Pipeline

A comprehensive Apache Spark ETL pipeline for QuickChat data lake architecture, implementing Bronze → Silver → Gold medallion architecture with Delta Lake.

## Related Services

This ETL pipeline works in conjunction with other QuickChat microservices:

| Service | Purpose | Relationship |
|---------|---------|--------------|
| **ml-service** | Real-time ML inference API | Consumes ML features from Gold layer |
| **analytics-service** | Business analytics | Consumes aggregated metrics from Gold layer |
| **insights-service** | User insights dashboard | Queries curated data from Gold layer |

The **spark-etl** pipeline generates ML feature vectors in the Gold layer, which are then used by **ml-service** for real-time predictions (smart replies, sentiment analysis, user engagement scoring).

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   spark-etl    │────▶│   Gold Layer    │────▶│   ml-service    │
│  (Batch ETL)   │     │  (Features DB)  │     │  (Inference)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                                               │
        │                                               ▼
        │                                       ┌─────────────────┐
        │                                       │ Real-time APIs  │
        │                                       │ - Smart Reply   │
        │                                       │ - Sentiment     │
        │                                       │ - Engagement    │
        │                                       └─────────────────┘
        ▼
┌─────────────────┐
│ analytics-svc  │
│ insights-svc   │
└─────────────────┘
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
├─────────────────────────────────────────────────────────────────────┤
│  MongoDB         │  MySQL          │  Kafka          │  S3          │
│  (Messages,      │  (Users,        │  (Real-time     │  (Media      │
│   Conversations, │   Auth)         │   Events)       │   Files)     │
│   Calls)         │                 │                 │              │
└────────┬─────────┴────────┬────────┴────────┬────────┴──────────────┘
         │                  │                 │
         ▼                  ▼                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                              │
│  • Raw data ingestion                                               │
│  • Minimal transformations                                          │
│  • Ingestion metadata added                                         │
│  • Partitioned by date                                              │
│  Format: Delta Lake                                                 │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER (Cleaned)                          │
│  • Data quality filtering                                           │
│  • Deduplication                                                    │
│  • Enrichment with derived fields                                   │
│  • Schema enforcement                                               │
│  Format: Delta Lake (Parquet)                                       │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER (Curated)                            │
│  • Business aggregations                                            │
│  • Daily/Weekly/Monthly metrics                                     │
│  • User engagement scores                                           │
│  • ML feature vectors                                               │
│  Format: Delta Lake                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
spark-etl/
├── build.sbt                           # SBT build configuration
├── project/
│   ├── build.properties               # SBT version
│   ├── plugins.sbt                    # SBT plugins
│   └── Dependencies.scala             # Dependency versions
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── application.conf       # Application configuration
│   │   └── scala/com/quickchat/etl/
│   │       ├── config/
│   │       │   └── AppConfig.scala    # Configuration case classes
│   │       ├── models/
│   │       │   └── Models.scala       # Data models
│   │       ├── transformers/
│   │       │   └── CommonTransformers.scala
│   │       ├── utils/
│   │       │   ├── SparkSessionBuilder.scala
│   │       │   └── DeltaLakeUtils.scala
│   │       └── jobs/
│   │           ├── bronze/
│   │           │   ├── BronzeMessagesJob.scala
│   │           │   ├── BronzeUsersJob.scala
│   │           │   ├── BronzeConversationsJob.scala
│   │           │   └── BronzeCallsJob.scala
│   │           ├── silver/
│   │           │   ├── SilverMessagesJob.scala
│   │           │   ├── SilverUsersJob.scala
│   │           │   └── SilverUserActivityJob.scala
│   │           ├── gold/
│   │           │   ├── DailyMetricsJob.scala
│   │           │   ├── UserEngagementJob.scala
│   │           │   └── MLFeaturesJob.scala
│   │           └── streaming/
│   │               └── KafkaStreamingJob.scala
│   └── test/
│       └── scala/com/quickchat/etl/   # Test files
├── docker/
│   ├── Dockerfile                     # Spark ETL Docker image
│   ├── docker-compose.yml             # Full infrastructure
│   ├── spark-defaults.conf            # Spark configuration
│   └── log4j2.properties              # Logging configuration
└── scripts/
    ├── run-etl.sh                     # ETL runner script
    └── airflow/dags/
        └── quickchat_etl_dag.py       # Airflow DAG
```

## 🚀 Quick Start

### Prerequisites

- Java 17+
- Scala 2.12
- SBT 1.9+
- Docker & Docker Compose
- Apache Spark 3.5.0

### Build

```bash
# Build the project
cd spark-etl
sbt clean compile

# Run tests
sbt test

# Build fat JAR
sbt assembly
```

### Run Locally

```bash
# Run a single job
./scripts/run-etl.sh bronze-messages

# Run all Bronze layer jobs
./scripts/run-etl.sh bronze-all

# Run complete pipeline
./scripts/run-etl.sh full-pipeline
```

### Run with Docker

```bash
# Start infrastructure
cd docker
docker-compose up -d

# Access UIs:
# - Spark Master: http://localhost:8080
# - Spark History: http://localhost:18080
# - MinIO Console: http://localhost:9001
# - Kafka UI: http://localhost:8090
# - Airflow: http://localhost:8085
```

## 📊 ETL Jobs

### Bronze Layer (Raw Data)

| Job | Description | Source | Schedule |
|-----|-------------|--------|----------|
| BronzeMessagesJob | Extract messages | MongoDB | Hourly |
| BronzeUsersJob | Extract users | MongoDB + MySQL | Daily |
| BronzeConversationsJob | Extract conversations | MongoDB | Daily |
| BronzeCallsJob | Extract calls | MongoDB | Hourly |

### Silver Layer (Cleaned)

| Job | Description | Dependencies | Schedule |
|-----|-------------|--------------|----------|
| SilverUsersJob | Clean & enrich users | BronzeUsers | Daily |
| SilverMessagesJob | Clean & enrich messages | BronzeMessages, SilverUsers | Hourly |
| SilverUserActivityJob | Calculate daily activity | SilverMessages | Daily |

### Gold Layer (Aggregated)

| Job | Description | Dependencies | Schedule |
|-----|-------------|--------------|----------|
| DailyMetricsJob | Platform-wide daily metrics | Silver layer | Daily |
| UserEngagementJob | Per-user engagement scores | Silver layer | Daily |
| MLFeaturesJob | ML feature vectors | Silver layer | Daily |

### Streaming

| Job | Description | Source | Mode |
|-----|-------------|--------|------|
| KafkaStreamingJob | Real-time ingestion | Kafka | Continuous |

## ⚙️ Configuration

### Environment Variables

```bash
# Data Lake
export DATA_LAKE_PATH=s3a://quickchat-data-lake

# MongoDB
export MONGODB_URI=mongodb://localhost:27017
export MONGODB_DATABASE=quickchat

# MySQL
export MYSQL_URL=jdbc:mysql://localhost:3306/quickchat_users
export MYSQL_USER=root
export MYSQL_PASSWORD=password

# Kafka
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# AWS S3
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1
```

## 📈 Metrics Generated

### Daily Metrics
- DAU (Daily Active Users)
- Total messages, calls
- Message types distribution
- Peak activity hours
- New users, conversations

### User Engagement
- Engagement score (0-100)
- Activity trend (increasing/stable/decreasing)
- Churn risk (low/medium/high)
- User segment (power_user/regular/casual/at_risk/churned)

### ML Features
- Activity features (messages, calls by time window)
- Social features (contacts, groups)
- Temporal features (preferred hours, weekday ratio)
- Velocity features (message rate trends)

## 🔧 Development

### Adding a New Job

1. Create job class in appropriate layer package
2. Extend common patterns from existing jobs
3. Add to Airflow DAG
4. Add to run-etl.sh script

### Testing

```bash
# Run all tests
sbt test

# Run specific test
sbt "testOnly *BronzeMessagesJobSpec"
```

## 📝 License

MIT License - QuickChat Team
