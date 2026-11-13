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

"""
Seeds and verifies the tables of the Iceberg pipeline example. See run_iceberg_pipeline.sh.

Usage:
    spark-submit iceberg_example_utils.py seed <warehouse> <batch>
    spark-submit iceberg_example_utils.py verify <warehouse> <number of source rows>
"""

import sys

from pyspark.sql import SparkSession

REGIONS = "array('NA', 'EU', 'APAC', 'LATAM')"


def seed(spark: SparkSession, batch: int) -> None:
    """Appends the rows with order_id in [batch * 100, (batch + 1) * 100) to ice.raw.orders."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.raw")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.sdp")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS ice.raw.orders "
        "(order_id BIGINT, region STRING, channel STRING, amount DOUBLE, order_ts TIMESTAMP) "
        "USING iceberg"
    )
    spark.sql(
        f"""
        INSERT INTO ice.raw.orders
        SELECT id,
               element_at({REGIONS}, CAST(id % 4 AS INT) + 1),
               IF(id % 2 = 0, 'online', 'store'),
               CAST((id % 50) * 2 AS DOUBLE),
               TIMESTAMP'2026-01-01 00:00:00' + make_interval(0, 0, 0, 0, 0, CAST(id AS INT), 0)
        FROM range({batch * 100}, {(batch + 1) * 100})
        """
    )


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"OK: {message}")


def verify(spark: SparkSession, expected_source_rows: int) -> None:
    """Checks every pipeline dataset against the source table."""
    source = spark.table("ice.raw.orders")
    check(source.count() == expected_source_rows, f"ice.raw.orders has {expected_source_rows} rows")

    bronze = spark.table("ice.sdp.orders_bronze")
    check(
        bronze.drop("ingested_at").exceptAll(source).isEmpty()
        and source.exceptAll(bronze.drop("ingested_at")).isEmpty(),
        "ice.sdp.orders_bronze has exactly the source rows",
    )

    expected_large = source.where("amount >= 50")
    large = spark.table("ice.sdp.large_orders").drop("ingested_at")
    check(
        large.exceptAll(expected_large).isEmpty() and expected_large.exceptAll(large).isEmpty(),
        "ice.sdp.large_orders has exactly the large source orders, without duplicates",
    )

    expected_revenue = spark.sql(
        "SELECT region, SUM(amount) AS revenue, COUNT(*) AS orders "
        "FROM ice.raw.orders GROUP BY region"
    )
    for name in ["ice.sdp.region_revenue", "spark_catalog.default.region_revenue_report"]:
        actual = spark.table(name)
        check(
            actual.exceptAll(expected_revenue).isEmpty()
            and expected_revenue.exceptAll(actual).isEmpty(),
            f"{name} matches the source aggregation",
        )

    summary = {r.channel: r.orders for r in spark.table("ice.sdp.channel_summary").collect()}
    expected_summary = {
        r["channel"]: r["count"] for r in expected_large.groupBy("channel").count().collect()
    }
    check(summary == expected_summary, "ice.sdp.channel_summary matches ice.sdp.large_orders")

    bronze_props = {
        r.key: r.value for r in spark.sql("SHOW TBLPROPERTIES ice.sdp.orders_bronze").collect()
    }
    check(bronze_props.get("format-version") == "2", "orders_bronze has format-version 2")
    check(
        bronze_props.get("write.parquet.compression-codec") == "zstd",
        "orders_bronze has the declared table properties",
    )
    partitions = spark.sql("SELECT * FROM ice.sdp.orders_bronze.partitions")
    check(
        partitions.select("partition.region").distinct().count() == 4,
        "orders_bronze is partitioned by region",
    )


if __name__ == "__main__":
    command, warehouse, arg = sys.argv[1], sys.argv[2], int(sys.argv[3])
    session = (
        SparkSession.builder.appName("IcebergPipelineExampleUtils")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", warehouse)
        .enableHiveSupport()
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    if command == "seed":
        seed(session, arg)
    elif command == "verify":
        verify(session, arg)
    else:
        raise ValueError(f"Unknown command: {command}")
    session.stop()
