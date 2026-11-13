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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(comment="Orders streamed from the existing Unity Catalog table sdp_example.raw.orders.")
def orders_bronze() -> DataFrame:
    # Created as sdp_example.sales.orders_bronze, from the catalog and schema of the pipeline spec.
    return spark.readStream.table("sdp_example.raw.orders").withColumn(  # noqa: F821
        "ingested_at", F.current_timestamp()
    )


@dp.materialized_view(name="sdp_example.raw.orders_snapshot")
def orders_snapshot() -> DataFrame:
    # A fully qualified name writes outside the pipeline's default schema.
    return spark.read.table("sdp_example.raw.orders").select(  # noqa: F821
        "order_id", "customer", "amount"
    )
