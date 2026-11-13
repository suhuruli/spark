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
Helper for run_uc_pipeline_example.sh: prepares and checks the Unity Catalog tables used by the
pipeline in this directory.

Usage: spark-submit <Unity Catalog confs> uc_example_sql.py <seed|append|check-initial|check-final>
"""

import sys

from pyspark.sql import SparkSession

CATALOG = "sdp_example"


def seed(spark: SparkSession) -> None:
    spark.sql(
        f"CREATE TABLE {CATALOG}.raw.orders "
        "(order_id BIGINT, customer STRING, amount DOUBLE, ts TIMESTAMP) USING delta"
    )
    spark.sql(
        f"INSERT INTO {CATALOG}.raw.orders VALUES "
        "(1, 'alice', 10.0, TIMESTAMP'2026-01-01 10:00:00'), "
        "(2, 'bob', 25.5, TIMESTAMP'2026-01-01 11:00:00'), "
        "(3, 'alice', 7.25, TIMESTAMP'2026-01-01 12:00:00')"
    )


def append(spark: SparkSession) -> None:
    spark.sql(
        f"INSERT INTO {CATALOG}.raw.orders VALUES "
        "(4, 'carol', 40.0, TIMESTAMP'2026-01-02 09:00:00'), "
        "(5, 'bob', 4.5, TIMESTAMP'2026-01-02 10:00:00')"
    )


def check(spark: SparkSession, expected_order_ids, expected_totals) -> None:
    def rows(query):
        return [tuple(r) for r in spark.sql(query).collect()]

    order_ids = rows(f"SELECT order_id FROM {CATALOG}.sales.orders_bronze ORDER BY order_id")
    assert order_ids == [(i,) for i in expected_order_ids], order_ids
    snapshot = rows(f"SELECT order_id FROM {CATALOG}.raw.orders_snapshot ORDER BY order_id")
    assert snapshot == [(i,) for i in expected_order_ids], snapshot
    totals = rows(
        "SELECT customer, orders, total_amount "
        f"FROM {CATALOG}.sales.customer_totals ORDER BY customer"
    )
    assert totals == expected_totals, totals
    for table in ["sales.orders_bronze", "sales.customer_totals", "raw.orders_snapshot"]:
        info = {r[0]: r[1] for r in rows(f"DESCRIBE TABLE EXTENDED {CATALOG}.{table}")}
        assert info.get("Provider") == "delta" and info.get("Type") == "MANAGED", (table, info)
    print(f"Checked {CATALOG}: orders {expected_order_ids}, totals {expected_totals}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SDPUnityCatalogExample").getOrCreate()
    action = sys.argv[1]
    if action == "seed":
        seed(spark)
    elif action == "append":
        append(spark)
    elif action == "check-initial":
        check(spark, [1, 2, 3], [("alice", 2, 17.25), ("bob", 1, 25.5)])
    elif action == "check-final":
        check(
            spark,
            [1, 2, 3, 4, 5],
            [("alice", 2, 17.25), ("bob", 2, 30.0), ("carol", 1, 40.0)],
        )
    else:
        raise ValueError(f"Unknown action: {action}")
    spark.stop()
