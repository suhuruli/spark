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

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


# Streaming table that incrementally ingests new snapshots of an existing Iceberg table.
# The table is partitioned and carries Iceberg table properties.
@dp.table(
    partition_cols=["region"],
    table_properties={"format-version": "2", "write.parquet.compression-codec": "zstd"},
    comment="Orders ingested incrementally from ice.raw.orders",
)
def orders_bronze() -> DataFrame:
    return spark.readStream.table("ice.raw.orders").withColumn("ingested_at", F.current_timestamp())


# Streaming table written by two append flows.
dp.create_streaming_table("large_orders", partition_cols=["channel"])


@dp.append_flow(target="large_orders")
def large_online_orders() -> DataFrame:
    return spark.readStream.table("orders_bronze").where("channel = 'online' AND amount >= 50")


@dp.append_flow(target="large_orders")
def large_store_orders() -> DataFrame:
    return spark.readStream.table("orders_bronze").where("channel = 'store' AND amount >= 50")


# Materialized view computed with a batch read, recomputed on every run.
@dp.materialized_view(partition_cols=["region"], table_properties={"team": "analytics"})
def region_revenue() -> DataFrame:
    return (
        spark.read.table("orders_bronze")
        .groupBy("region")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
    )


# Materialized view in the session catalog that reads an Iceberg table (multi-catalog pipeline).
@dp.materialized_view(name="spark_catalog.default.region_revenue_report", format="parquet")
def region_revenue_report() -> DataFrame:
    return spark.read.table("region_revenue").orderBy("region")
