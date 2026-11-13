#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Runs a Spark Declarative Pipeline that reads and writes Apache Iceberg tables, and checks its
# results after an initial run, an incremental run, and a full refresh.
#
# Iceberg is not a Spark dependency. Provide the iceberg-spark-runtime jar that matches the Spark
# version in SPARK_HOME, for example iceberg-spark-runtime-4.1_2.13-1.12.0.jar for Spark 4.1:
#
#   ICEBERG_JAR=/path/to/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
#     examples/src/main/python/pipelines/iceberg/run_iceberg_pipeline.sh [work dir]
#
# The pipeline uses an Iceberg Hadoop catalog named `ice` with a local warehouse, plus the session
# catalog (`spark_catalog`) backed by an embedded Hive metastore, all created under the work dir.
# The Hive metastore makes `spark_catalog` tables survive between runs; the default in-memory
# session catalog loses them when `spark-pipelines` exits.

set -euo pipefail

if [ -z "${ICEBERG_JAR:-}" ] || [ ! -f "$ICEBERG_JAR" ]; then
  echo "Set ICEBERG_JAR to the path of an iceberg-spark-runtime jar." >&2
  exit 1
fi

EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -z "${SPARK_HOME:-}" ]; then
  SPARK_HOME="$(cd "$EXAMPLE_DIR/../../../../../.." && pwd)"
fi
if [ -n "${1:-}" ]; then
  mkdir -p "$1"
  WORK_DIR="$(cd "$1" && pwd)"
else
  WORK_DIR="$(mktemp -d)"
fi
WAREHOUSE="file://$WORK_DIR/iceberg-warehouse"
PROJECT_DIR="$WORK_DIR/iceberg_pipeline"

COMMON_CONF=(
  --jars "$ICEBERG_JAR"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalogImplementation=hive
  --conf "spark.sql.warehouse.dir=$WORK_DIR/spark-warehouse"
)

mkdir -p "$PROJECT_DIR"
cp -r "$EXAMPLE_DIR/transformations" "$PROJECT_DIR/"
cat > "$PROJECT_DIR/spark-pipeline.yml" <<SPEC
name: iceberg_pipeline
storage: file://$WORK_DIR/pipeline-storage
catalog: ice
database: sdp
configuration:
  spark.sql.catalog.ice: org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.ice.type: hadoop
  spark.sql.catalog.ice.warehouse: $WAREHOUSE
libraries:
  - glob:
      include: transformations/**
SPEC

# Every step runs in the project dir, so that they share the embedded Hive metastore (metastore_db)
# that is created in the current directory.
utils() {
  (cd "$PROJECT_DIR" && "$SPARK_HOME/bin/spark-submit" "${COMMON_CONF[@]}" \
    "$EXAMPLE_DIR/iceberg_example_utils.py" "$1" "$WAREHOUSE" "$2")
}

run_pipeline() {
  (cd "$PROJECT_DIR" && "$SPARK_HOME/bin/spark-pipelines" run "${COMMON_CONF[@]}" "$@")
}

echo "Work dir: $WORK_DIR"

echo "== Initial run"
utils seed 0
run_pipeline
utils verify 100

echo "== Incremental run after new source data"
utils seed 1
run_pipeline
utils verify 200

echo "== Full refresh"
utils seed 2
run_pipeline --full-refresh-all
utils verify 300

echo "Iceberg pipeline example finished successfully."
