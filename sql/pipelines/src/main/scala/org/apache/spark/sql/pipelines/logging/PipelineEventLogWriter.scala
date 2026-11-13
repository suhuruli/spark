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

import java.io.ByteArrayOutputStream
import java.util.UUID

import scala.util.control.NonFatal

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}

import org.apache.spark.SparkThrowable
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.pipelines.common.RunState
import org.apache.spark.sql.types.StructType

/**
 * Persists [[PipelineEvent]]s as JSON lines so that the history of pipeline runs can be queried
 * with Spark, e.g. `spark.read.schema(PipelineEventLogWriter.SCHEMA).json(eventLogDir)`.
 *
 * Each run writes to its own file under `eventLogDir`. A run starts with the first event written
 * after this writer is created or after the previous run ended, and ends with a terminal
 * [[RunProgress]] event (COMPLETED, FAILED or CANCELED), which closes the file. Each event is
 * flushed as it is written. On file systems without flush support (e.g. object stores) the file
 * becomes visible when the run ends.
 *
 * Failures to write the event log are logged and do not fail the pipeline.
 *
 * @param eventLogDir The directory that holds one event log file per run.
 * @param hadoopConf The Hadoop configuration used to access `eventLogDir`.
 */
class PipelineEventLogWriter(eventLogDir: Path, hadoopConf: Configuration) extends Logging {
  import PipelineEventLogWriter._

  // The file and id of the run in progress, if any. Guarded by `this`.
  private var out: Option[FSDataOutputStream] = None
  private var runId: String = _

  /** Appends `event` to the event log file of the current run. */
  def write(event: PipelineEvent): Unit = synchronized {
    try {
      val stream = out.getOrElse(startRun(event))
      stream.write(toJson(event, runId))
      stream.write('\n')
      stream.hflush()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to write pipeline event to event log directory " +
          log"${MDC(LogKeys.PATH, eventLogDir)}", e)
        closeQuietly()
    }
    event.details match {
      case RunProgress(RunState.COMPLETED | RunState.FAILED | RunState.CANCELED) =>
        closeQuietly()
      case _ =>
    }
  }

  private def startRun(firstEvent: PipelineEvent): FSDataOutputStream = {
    runId = UUID.randomUUID().toString
    val file = new Path(eventLogDir, s"${firstEvent.timestamp.getTime}-$runId.json")
    val stream = file.getFileSystem(hadoopConf).create(file, false)
    out = Some(stream)
    stream
  }

  private def closeQuietly(): Unit = {
    try {
      out.foreach(_.close())
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to close pipeline event log file in " +
          log"${MDC(LogKeys.PATH, eventLogDir)}", e)
    } finally {
      out = None
    }
  }
}

object PipelineEventLogWriter {

  /** The name of the directory under the pipeline storage root that holds the event log. */
  val EVENT_LOG_DIR_NAME = "_event_log"

  /** The schema of the records in the event log. */
  val SCHEMA: StructType = StructType.fromDDL(
    "id STRING, run_id STRING, timestamp TIMESTAMP, level STRING, event_type STRING, " +
      "message STRING, flow_name STRING, dataset_name STRING, source_file STRING, " +
      "source_line INT, flow_status STRING, run_state STRING, error_class STRING, " +
      "error_message STRING")

  private val jsonFactory = new JsonFactory()

  /** Serializes `event` as a single-line JSON object matching [[SCHEMA]]. */
  private[logging] def toJson(event: PipelineEvent, runId: String): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    val gen = jsonFactory.createGenerator(buffer, JsonEncoding.UTF8)
    try {
      def field(name: String, value: Option[String]): Unit = value match {
        case Some(v) => gen.writeStringField(name, v)
        case None => gen.writeNullField(name)
      }
      val (eventType, flowStatus, runState) = event.details match {
        case FlowProgress(status) => ("flow_progress", Some(status.toString), None)
        case RunProgress(state) => ("run_progress", None, Some(state.toString))
      }
      val sourceCodeLocation = event.origin.sourceCodeLocation

      gen.writeStartObject()
      gen.writeStringField("id", event.id)
      gen.writeStringField("run_id", runId)
      gen.writeStringField("timestamp", event.timestamp.toInstant.toString)
      gen.writeStringField("level", event.level.toString)
      gen.writeStringField("event_type", eventType)
      gen.writeStringField("message", event.message)
      field("flow_name", event.origin.flowName)
      field("dataset_name", event.origin.datasetName)
      field("source_file", sourceCodeLocation.flatMap(_.filePath))
      sourceCodeLocation.flatMap(_.line) match {
        case Some(line) => gen.writeNumberField("source_line", line)
        case None => gen.writeNullField("source_line")
      }
      field("flow_status", flowStatus)
      field("run_state", runState)
      field("error_class", event.error.collect {
        case e: SparkThrowable if e.getCondition != null => e.getCondition
      })
      field("error_message", Option.when(event.error.nonEmpty)(event.errorMessages.mkString("\n")))
      gen.writeEndObject()
    } finally {
      gen.close()
    }
    buffer.toByteArray
  }
}
