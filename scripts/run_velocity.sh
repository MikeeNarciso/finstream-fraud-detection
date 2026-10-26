#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=".env"

if [[ -f "$ENV_FILE" ]]; then
    set -a # Automatically export all variables defined
    source "$ENV_FILE"
    set +a # Disable auto-export
else
    echo "Warning: $ENV_FILE not found. Relying on existing environment variables." >&2
fi

# 1. Configuration (override via environment variables if desired)
AWS_PROFILE="${AWS_PROFILE:-default}"
AWS_REGION="${AWS_REGION}"
S3_BUCKET_NAME="${S3_BUCKET_NAME}"
CONTAINER_NAME="finstream-spark-master"
SPARK_SCRIPT_PATH="/opt/spark/src/streaming/fraud_velocity_job.py"

if [[ -z "$AWS_REGION" || -z "$S3_BUCKET_NAME" ]]; then
    echo "Error: AWS_REGION and S3_BUCKET_NAME must be set in the environment or $ENV_FILE." >&2
    exit 1
fi

echo "=== FinStream Iceberg Pipeline Runner ==="

# 2. Extract temporary credentials from AWS SSO cache
echo "Fetching AWS credentials for profile: ${AWS_PROFILE}..."
if ! CREDS_JSON=$(aws configure export-credentials --profile "${AWS_PROFILE}" 2>/dev/null); then
  echo "Error: Failed to export AWS credentials. Did your SSO session expire?"
  echo "Run: aws sso login --profile ${AWS_PROFILE}"
  exit 1
fi

# 3. Parse JSON values using inline Python
export AWS_ACCESS_KEY_ID=$(echo "${CREDS_JSON}" | python -c "import sys, json; print(json.load(sys.stdin)['AccessKeyId'])")
export AWS_SECRET_ACCESS_KEY=$(echo "${CREDS_JSON}" | python -c "import sys, json; print(json.load(sys.stdin)['SecretAccessKey'])")
export AWS_SESSION_TOKEN=$(echo "${CREDS_JSON}" | python -c "import sys, json; print(json.load(sys.stdin)['SessionToken'])")

echo "Credentials exported successfully. Submitting Spark Structured Streaming job..."

# 4. Execute spark-submit inside container
# MSYS_NO_PATHCONV=1 prevents Git Bash from altering Linux container paths
MSYS_NO_PATHCONV=1 docker exec \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  -e AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN}" \
  -e AWS_REGION="${AWS_REGION}" \
  -e S3_BUCKET_NAME="${S3_BUCKET_NAME}" \
  -it "${CONTAINER_NAME}" /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  --conf "spark.executorEnv.AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
  --conf "spark.executorEnv.AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
  --conf "spark.executorEnv.AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}" \
  --conf "spark.executorEnv.AWS_REGION=${AWS_REGION}" \
  --driver-memory 512M \
  --executor-memory 1G \
  --total-executor-cores 1 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  "${SPARK_SCRIPT_PATH}"