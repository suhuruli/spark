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

# Runs the Spark Declarative Pipelines example in this directory against an open-source Unity
# Catalog server (https://www.unitycatalog.io) and checks the results. This is a manual
# integration test: the Unity Catalog connector and Delta Lake are not Spark dependencies, so
# it is not part of Spark's test suites.
#
# Requirements:
#   - A running Unity Catalog server whose catalogs can use local managed storage, for example
#     `bin/start-uc-server` from a Unity Catalog release.
#   - SPARK_HOME pointing to a Spark distribution with Spark Connect (`spark-pipelines`).
#   - SDP_UC_PACKAGES set to the Maven coordinates of the Delta Lake and Unity Catalog Spark
#     connectors built for the Spark version of SPARK_HOME, for example:
#       io.delta:delta-spark_4.2_2.13:4.4.1,io.unitycatalog:unitycatalog-spark_4.2_2.13:0.7.0
#
# Optional environment variables:
#   UC_URI           Unity Catalog server URI (default: http://localhost:8080)
#   UC_TOKEN         Unity Catalog access token (default: empty, for servers without auth)
#   SDP_UC_WORK_DIR  Directory for the pipeline project, storage and managed tables
#                    (default: a new temporary directory)
#   SDP_UC_SPARK_ARGS  Extra spark-submit arguments, for example
#                    "--conf spark.jars.ivySettings=/path/to/ivysettings.xml"
#
# The script drops and recreates the Unity Catalog catalog `sdp_example`, then:
#   1. creates the schemas `raw` and `sales` and the source table `sdp_example.raw.orders`,
#   2. runs the pipeline and checks its tables,
#   3. appends to the source table, runs the pipeline incrementally and checks the tables,
#   4. runs a full refresh of all tables and checks that nothing was duplicated.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
UC_URI="${UC_URI:-http://localhost:8080}"
UC_TOKEN="${UC_TOKEN:-}"
CATALOG="sdp_example"

if [ -z "${SPARK_HOME:-}" ]; then
  echo "SPARK_HOME must be set." >&2
  exit 1
fi
if [ -z "${SDP_UC_PACKAGES:-}" ]; then
  echo "SDP_UC_PACKAGES must be set to the Delta Lake and Unity Catalog connector packages." >&2
  exit 1
fi

WORK_DIR="${SDP_UC_WORK_DIR:-$(mktemp -d -t sdp-uc-example-XXXXXX)}"
mkdir -p "$WORK_DIR"
WORK_DIR="$(cd "$WORK_DIR" && pwd)"
echo "Working directory: $WORK_DIR"

uc_api() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -f -X "$method" -H "Content-Type: application/json")
  if [ -n "$UC_TOKEN" ]; then
    args+=(-H "Authorization: Bearer $UC_TOKEN")
  fi
  if [ -n "$body" ]; then
    args+=(-d "$body")
  fi
  curl "${args[@]}" "$UC_URI/api/2.1/unity-catalog/$path" > /dev/null
}

echo "Creating catalog $CATALOG with schemas raw and sales"
uc_api DELETE "catalogs/$CATALOG?force=true" || true
uc_api POST catalogs \
  "{\"name\": \"$CATALOG\", \"storage_root\": \"file://$WORK_DIR/managed\"}"
uc_api POST schemas "{\"name\": \"raw\", \"catalog_name\": \"$CATALOG\"}"
uc_api POST schemas "{\"name\": \"sales\", \"catalog_name\": \"$CATALOG\"}"

# Static configurations must be set when the Spark Connect server starts, so they are passed on
# the command line. The catalog configurations could also go in the `configuration` section of
# the pipeline spec.
# shellcheck disable=SC2206
SPARK_ARGS=(
  --packages "$SDP_UC_PACKAGES"
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
  --conf "spark.sql.catalog.$CATALOG=io.unitycatalog.spark.UCSingleCatalog"
  --conf "spark.sql.catalog.$CATALOG.uri=$UC_URI"
  --conf "spark.sql.catalog.$CATALOG.token=$UC_TOKEN"
  ${SDP_UC_SPARK_ARGS:-}
)

run_sql() {
  (cd "$WORK_DIR" && "$SPARK_HOME/bin/spark-submit" "${SPARK_ARGS[@]}" \
    --conf spark.log.level=WARN "$EXAMPLE_DIR/uc_example_sql.py" "$1")
}

PROJECT_DIR="$WORK_DIR/project"
mkdir -p "$PROJECT_DIR"
cp -r "$EXAMPLE_DIR/transformations" "$PROJECT_DIR/"
cat > "$PROJECT_DIR/spark-pipeline.yml" <<EOF
name: sdp_unity_catalog_example
storage: file://$WORK_DIR/pipeline-storage
catalog: $CATALOG
schema: sales
configuration:
  # Unity Catalog only supports Delta for managed tables.
  spark.sql.sources.default: delta
libraries:
  - glob:
      include: transformations/**
EOF

run_pipeline() {
  (cd "$PROJECT_DIR" && "$SPARK_HOME/bin/spark-pipelines" "${SPARK_ARGS[@]}" run "$@")
}

echo "Creating the source table $CATALOG.raw.orders"
run_sql seed

echo "Running the pipeline"
run_pipeline
run_sql check-initial

echo "Appending to the source table and running the pipeline incrementally"
run_sql append
run_pipeline
run_sql check-final

echo "Running a full refresh"
run_pipeline --full-refresh-all
run_sql check-final

echo "The Unity Catalog pipeline example passed."
