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

package org.apache.spark.sql.pipelines.logging

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pipelines.common.RunState
import org.apache.spark.sql.pipelines.graph.PipelineUpdateContextImpl
import org.apache.spark.sql.pipelines.utils.PipelineTest
import org.apache.spark.sql.test.SharedSparkSession

class PipelineEventLogSuite extends PipelineTest with SharedSparkSession {
  import testImplicits._

  private def eventLogDir: String =
    new Path(storageRoot, PipelineEventLogWriter.EVENT_LOG_DIR_NAME).toString

  /** The event log files, excluding hidden files such as local file system checksums. */
  private def eventLogFiles: Seq[File] =
    Option(new File(new Path(eventLogDir).toUri).listFiles()).toSeq.flatten
      .filterNot(_.getName.startsWith("."))

  private def readEventLog(): DataFrame =
    spark.read.schema(PipelineEventLogWriter.SCHEMA).json(eventLogDir)

  private def runPipeline(sqlText: String): Unit = {
    val context = new PipelineUpdateContextImpl(
      unresolvedDataflowGraphFromSql(sqlText), eventCallback = _ => (), storageRoot = storageRoot)
    context.pipelineExecution.runPipeline()
  }

  test("event log is not written by default") {
    runPipeline("CREATE MATERIALIZED VIEW mv AS SELECT 1 AS id")
    assert(spark.table(fullyQualifiedIdentifier("mv").quotedString).count() == 1)
    assert(eventLogFiles.isEmpty)
  }

  test("successful run writes flow and run progress events") {
    withSQLConf(SQLConf.PIPELINES_EVENT_LOG_ENABLED.key -> "true") {
      runPipeline("CREATE MATERIALIZED VIEW mv AS SELECT 1 AS id")
    }
    assert(eventLogFiles.size == 1)

    val events = readEventLog().orderBy("timestamp").collect()
    assert(events.map(_.getAs[String]("run_id")).distinct.length == 1)
    events.foreach { e =>
      assert(e.getAs[String]("id") != null)
      assert(e.getAs[java.sql.Timestamp]("timestamp") != null)
      assert(e.getAs[String]("message") != null)
    }

    val mvName = eventLogName("mv")
    checkAnswer(
      readEventLog()
        .where("event_type = 'flow_progress'")
        .select("flow_name", "flow_status")
        .where("flow_status = 'COMPLETED'"),
      Row(mvName, "COMPLETED"))
    assert(readEventLog().where(s"flow_name = '$mvName'")
      .select("source_file").distinct().collect().toSeq == Seq(Row("dataset.sql")))

    val last = events.last
    assert(last.getAs[String]("event_type") == "run_progress")
    assert(last.getAs[String]("run_state") == "COMPLETED")
    assert(last.getAs[String]("level") == "INFO")
    assert(last.getAs[String]("error_message") == null)
  }

  test("failed run records the failing flow and the failure cause") {
    withSQLConf(SQLConf.PIPELINES_EVENT_LOG_ENABLED.key -> "true") {
      runPipeline("CREATE MATERIALIZED VIEW mv AS SELECT * FROM table_that_does_not_exist")
    }

    val failedFlow = readEventLog()
      .where("event_type = 'flow_progress' AND flow_status = 'FAILED'")
      .collect()
    assert(failedFlow.length == 1)
    assert(failedFlow.head.getAs[String]("flow_name") == eventLogName("mv"))
    assert(failedFlow.head.getAs[String]("error_class") == "TABLE_OR_VIEW_NOT_FOUND")
    assert(failedFlow.head.getAs[String]("error_message").contains("table_that_does_not_exist"))

    checkAnswer(
      readEventLog().where("event_type = 'run_progress'").select("run_state", "level"),
      Row("FAILED", "ERROR"))
  }

  test("each run writes a separate event log file") {
    withSQLConf(SQLConf.PIPELINES_EVENT_LOG_ENABLED.key -> "true") {
      val context = new PipelineUpdateContextImpl(
        unresolvedDataflowGraphFromSql("CREATE MATERIALIZED VIEW mv AS SELECT 1 AS id"),
        eventCallback = _ => (),
        storageRoot = storageRoot)
      context.pipelineExecution.runPipeline()
      context.pipelineExecution.runPipeline()
    }
    assert(eventLogFiles.size == 2)
    checkAnswer(
      readEventLog()
        .where("event_type = 'run_progress'")
        .groupBy("run_state")
        .agg(countDistinct("run_id")),
      Row("COMPLETED", 2L))
  }

  test("events with no origin or error serialize optional fields as null") {
    val event = ConstructPipelineEvent(
      origin = PipelineEventOrigin(datasetName = None, flowName = None, sourceCodeLocation = None),
      level = EventLevel.INFO,
      message = "msg",
      details = RunProgress(RunState.COMPLETED))
    val json = new String(PipelineEventLogWriter.toJson(event, "run-1"), "UTF-8")
    assert(!json.contains("\n"))
    val row = spark.read.schema(PipelineEventLogWriter.SCHEMA).json(Seq(json).toDS()).head()
    assert(row.getAs[String]("run_id") == "run-1")
    assert(row.getAs[String]("event_type") == "run_progress")
    Seq("flow_name", "dataset_name", "source_file", "source_line", "flow_status", "error_class",
      "error_message").foreach(f => assert(row.isNullAt(row.fieldIndex(f)), f))
  }
}
