#!/bin/bash

# QuickChat Spark ETL Runner Script
# Usage: ./run-etl.sh <job-type> [options]

set -e

# Configuration
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DRIVER_MEMORY="${DRIVER_MEMORY:-2g}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
NUM_EXECUTORS="${NUM_EXECUTORS:-2}"
JAR_PATH="${JAR_PATH:-target/scala-2.12/quickchat-spark-etl.jar}"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Log functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Show usage
show_usage() {
    echo "QuickChat Spark ETL Runner"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  bronze-all          Run all Bronze layer jobs"
    echo "  bronze-messages     Run Bronze Messages job"
    echo "  bronze-users        Run Bronze Users job"
    echo "  bronze-conversations Run Bronze Conversations job"
    echo "  bronze-calls        Run Bronze Calls job"
    echo ""
    echo "  silver-all          Run all Silver layer jobs"
    echo "  silver-messages     Run Silver Messages job"
    echo "  silver-users        Run Silver Users job"
    echo "  silver-activity     Run Silver User Activity job"
    echo ""
    echo "  gold-all            Run all Gold layer jobs"
    echo "  gold-daily          Run Daily Metrics job"
    echo "  gold-engagement     Run User Engagement job"
    echo "  gold-ml-features    Run ML Features job"
    echo ""
    echo "  streaming           Start Kafka Streaming job"
    echo "  full-pipeline       Run complete ETL pipeline"
    echo ""
    echo "Options:"
    echo "  --master <url>      Spark master URL (default: local[*])"
    echo "  --driver-mem <mem>  Driver memory (default: 2g)"
    echo "  --executor-mem <mem> Executor memory (default: 2g)"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 bronze-messages"
    echo "  $0 full-pipeline --master spark://localhost:7077"
    echo "  $0 streaming --driver-mem 4g"
}

# Spark submit function
spark_submit() {
    local main_class=$1
    local job_name=$2

    log_info "Starting job: $job_name"
    log_info "Main class: $main_class"
    log_info "Spark master: $SPARK_MASTER"

    spark-submit \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --driver-memory "$DRIVER_MEMORY" \
        --executor-memory "$EXECUTOR_MEMORY" \
        --executor-cores "$EXECUTOR_CORES" \
        --num-executors "$NUM_EXECUTORS" \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.app.name="QuickChat-ETL-$job_name" \
        --class "$main_class" \
        "$JAR_PATH"

    if [ $? -eq 0 ]; then
        log_info "Job $job_name completed successfully"
    else
        log_error "Job $job_name failed"
        exit 1
    fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --master)
            SPARK_MASTER="$2"
            shift 2
            ;;
        --driver-mem)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        --executor-mem)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            COMMAND=$1
            shift
            ;;
    esac
done

# Check if JAR exists
if [ ! -f "$JAR_PATH" ]; then
    log_warn "JAR file not found at $JAR_PATH"
    log_info "Building project..."
    sbt assembly
fi

# Execute command
case $COMMAND in
    # Bronze Layer
    bronze-messages)
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeMessagesJob" "BronzeMessages"
        ;;
    bronze-users)
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeUsersJob" "BronzeUsers"
        ;;
    bronze-conversations)
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeConversationsJob" "BronzeConversations"
        ;;
    bronze-calls)
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeCallsJob" "BronzeCalls"
        ;;
    bronze-all)
        log_info "Running all Bronze layer jobs..."
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeMessagesJob" "BronzeMessages"
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeUsersJob" "BronzeUsers"
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeConversationsJob" "BronzeConversations"
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeCallsJob" "BronzeCalls"
        ;;

    # Silver Layer
    silver-messages)
        spark_submit "com.quickchat.etl.jobs.silver.SilverMessagesJob" "SilverMessages"
        ;;
    silver-users)
        spark_submit "com.quickchat.etl.jobs.silver.SilverUsersJob" "SilverUsers"
        ;;
    silver-activity)
        spark_submit "com.quickchat.etl.jobs.silver.SilverUserActivityJob" "SilverUserActivity"
        ;;
    silver-all)
        log_info "Running all Silver layer jobs..."
        spark_submit "com.quickchat.etl.jobs.silver.SilverUsersJob" "SilverUsers"
        spark_submit "com.quickchat.etl.jobs.silver.SilverMessagesJob" "SilverMessages"
        spark_submit "com.quickchat.etl.jobs.silver.SilverUserActivityJob" "SilverUserActivity"
        ;;

    # Gold Layer
    gold-daily)
        spark_submit "com.quickchat.etl.jobs.gold.DailyMetricsJob" "DailyMetrics"
        ;;
    gold-engagement)
        spark_submit "com.quickchat.etl.jobs.gold.UserEngagementJob" "UserEngagement"
        ;;
    gold-ml-features)
        spark_submit "com.quickchat.etl.jobs.gold.MLFeaturesJob" "MLFeatures"
        ;;
    gold-all)
        log_info "Running all Gold layer jobs..."
        spark_submit "com.quickchat.etl.jobs.gold.DailyMetricsJob" "DailyMetrics"
        spark_submit "com.quickchat.etl.jobs.gold.UserEngagementJob" "UserEngagement"
        spark_submit "com.quickchat.etl.jobs.gold.MLFeaturesJob" "MLFeatures"
        ;;

    # Streaming
    streaming)
        spark_submit "com.quickchat.etl.jobs.streaming.KafkaStreamingJob" "KafkaStreaming"
        ;;

    # Full Pipeline
    full-pipeline)
        log_info "Running complete ETL pipeline..."
        log_info "Step 1/3: Bronze Layer"
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeMessagesJob" "BronzeMessages"
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeUsersJob" "BronzeUsers"
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeConversationsJob" "BronzeConversations"
        spark_submit "com.quickchat.etl.jobs.bronze.BronzeCallsJob" "BronzeCalls"

        log_info "Step 2/3: Silver Layer"
        spark_submit "com.quickchat.etl.jobs.silver.SilverUsersJob" "SilverUsers"
        spark_submit "com.quickchat.etl.jobs.silver.SilverMessagesJob" "SilverMessages"
        spark_submit "com.quickchat.etl.jobs.silver.SilverUserActivityJob" "SilverUserActivity"

        log_info "Step 3/3: Gold Layer"
        spark_submit "com.quickchat.etl.jobs.gold.DailyMetricsJob" "DailyMetrics"
        spark_submit "com.quickchat.etl.jobs.gold.UserEngagementJob" "UserEngagement"
        spark_submit "com.quickchat.etl.jobs.gold.MLFeaturesJob" "MLFeatures"

        log_info "Full pipeline completed successfully!"
        ;;

    *)
        log_error "Unknown command: $COMMAND"
        show_usage
        exit 1
        ;;
esac
