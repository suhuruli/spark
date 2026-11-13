/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.pipelines.util

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class SparkSessionUtilsSuite extends QueryTest with SharedSparkSession {

  test("withSqlConf sets the configurations and restores them afterwards") {
    val unsetKey = "spark.sql.pipelines.test.unset"
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "7") {
      SparkSessionUtils.withSqlConf(
        spark,
        SQLConf.SHUFFLE_PARTITIONS.key -> "3",
        unsetKey -> "v") {
        assert(spark.conf.get(SQLConf.SHUFFLE_PARTITIONS.key) == "3")
        assert(spark.conf.get(unsetKey) == "v")
      }
      assert(spark.conf.get(SQLConf.SHUFFLE_PARTITIONS.key) == "7")
      assert(spark.conf.getOption(unsetKey).isEmpty)
    }
  }

  test("withSqlConf accepts a static configuration that already has the requested value") {
    val staticKey = StaticSQLConf.GLOBAL_TEMP_DATABASE.key
    val currentValue = spark.conf.get(staticKey)
    val result = SparkSessionUtils.withSqlConf(spark, staticKey -> currentValue) {
      spark.conf.get(staticKey)
    }
    assert(result == currentValue)
    assert(spark.conf.get(staticKey) == currentValue)
  }

  test("withSqlConf rejects a static configuration with a different value") {
    val staticKey = StaticSQLConf.GLOBAL_TEMP_DATABASE.key
    val e = intercept[AnalysisException] {
      SparkSessionUtils.withSqlConf(spark, staticKey -> "other_global_temp")(())
    }
    checkError(
      exception = e,
      condition = "CANNOT_MODIFY_STATIC_CONFIG",
      parameters = Map("key" -> ("\"" + staticKey + "\""))
    )
  }
}
