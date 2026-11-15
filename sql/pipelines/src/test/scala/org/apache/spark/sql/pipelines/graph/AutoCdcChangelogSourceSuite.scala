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

package org.apache.spark.sql.pipelines.graph

import java.util.{HashMap => JHashMap}

import org.apache.spark.sql.{functions => F, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.{
  ChangelogProperties,
  Column => V2Column,
  Identifier,
  InMemoryChangelogCatalog
}
import org.apache.spark.sql.connector.catalog.Changelog.{
  CHANGE_TYPE_DELETE,
  CHANGE_TYPE_INSERT,
  CHANGE_TYPE_UPDATE_POSTIMAGE,
  CHANGE_TYPE_UPDATE_PREIMAGE
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.pipelines.autocdc.{
  AutoCdcReservedNames,
  ColumnSelection,
  ScdType,
  UnqualifiedColumnName
}
import org.apache.spark.sql.pipelines.utils.{ExecutionTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * End-to-end tests for AutoCDC flows whose source is a changelog read
 * (`DataStreamReader.changes` or `STREAM ... CHANGES`). The flow takes its delete detection,
 * metadata-column exclusion and `update_preimage` filtering from the
 * [[org.apache.spark.sql.connector.catalog.Changelog]] contract, and a read that AutoCDC cannot
 * reconcile deterministically is rejected when the pipeline is analyzed.
 */
class AutoCdcChangelogSourceSuite
    extends ExecutionTest
    with SharedSparkSession
    with AutoCdcGraphExecutionTestMixin {

  /** A second catalog, backed by [[InMemoryChangelogCatalog]], serving the changelog sources. */
  private val cdcCatalog: String = "cdc"
  private val cdcNamespace: String = "cdcns"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(s"spark.sql.catalog.$cdcCatalog", classOf[InMemoryChangelogCatalog].getName)
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $cdcCatalog.$cdcNamespace")
  }

  override protected def afterEach(): Unit = {
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$cdcCatalog")
    super.afterEach()
  }

  private def changelogCatalog: InMemoryChangelogCatalog =
    spark.sessionState.catalogManager.catalog(cdcCatalog).asInstanceOf[InMemoryChangelogCatalog]

  /** One change of an `(id, name)` row, optionally carrying the connector's row version. */
  private case class Change(
      id: Long,
      name: String,
      changeType: String,
      commitVersion: Long,
      rowVersion: Option[Long] = None)

  /**
   * Creates the changelog source table `cdc.cdcns.<table>` with data columns `(id BIGINT, name
   * STRING)`, plus `row_version BIGINT` when any change carries a row version, and adds the given
   * changes to it. Every change of a commit shares the commit's timestamp, as the changelog
   * contract requires.
   */
  private def createChangelogSource(
      table: String,
      changes: Seq[Change],
      properties: ChangelogProperties = ChangelogProperties()): Unit = {
    val withRowVersion = changes.exists(_.rowVersion.isDefined)
    val dataColumns = Seq(V2Column.create("id", LongType), V2Column.create("name", StringType)) ++
      Option.when(withRowVersion)(V2Column.create("row_version", LongType))
    val ident = Identifier.of(Array(cdcNamespace), table)
    changelogCatalog.createTable(
      ident,
      dataColumns.toArray,
      Array.empty[Transform],
      new JHashMap[String, String]())
    changelogCatalog.setChangelogProperties(ident, properties)
    changelogCatalog.addChangeRows(
      ident,
      changes.map { change =>
        val dataValues = Seq[Any](change.id, UTF8String.fromString(change.name)) ++
          Option.when(withRowVersion)(change.rowVersion.get)
        val metadataValues = Seq[Any](
          UTF8String.fromString(change.changeType),
          change.commitVersion,
          change.commitVersion * 1000000L)
        InternalRow.fromSeq(dataValues ++ metadataValues)
      })
  }

  /** A streaming `changes()` read of the changelog source table `cdc.cdcns.<table>`. */
  private def changelogStream(table: String, options: Map[String, String] = Map.empty): DataFrame =
    spark.readStream
      .options(Map("startingVersion" -> "1") ++ options)
      .changes(s"$cdcCatalog.$cdcNamespace.$table")

  private def targetTable: String = s"$catalog.$namespace.target"

  /** Creates the SCD1 target `(id, name)` with the AutoCDC metadata column. */
  private def createScd1Target(): Unit =
    spark.sql(s"CREATE TABLE $targetTable (id BIGINT NOT NULL, name STRING, $scd1MetadataDdl)")

  /**
   * The changes used by most tests: `alice` is inserted then updated, `bob` is inserted then
   * deleted, `carol` is inserted. Each key ends in a different state (current, deleted, inserted).
   */
  private val insertUpdateDeleteChanges: Seq[Change] = Seq(
    Change(1L, "alice", CHANGE_TYPE_INSERT, 1L),
    Change(2L, "bob", CHANGE_TYPE_INSERT, 1L),
    Change(1L, "alice", CHANGE_TYPE_UPDATE_PREIMAGE, 2L),
    Change(1L, "alice2", CHANGE_TYPE_UPDATE_POSTIMAGE, 2L),
    Change(3L, "carol", CHANGE_TYPE_INSERT, 2L),
    Change(2L, "bob", CHANGE_TYPE_DELETE, 3L)
  )

  test("SCD1: inserts, updates and deletes of a changelog converge to each key's current state") {
    createScd1Target()
    createChangelogSource("src", insertUpdateDeleteChanges)

    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = F.col("_commit_version")
    ))

    // The update's post-image wins over its pre-image at the same commit, the delete removes
    // `bob`, and the changelog metadata columns do not reach the target.
    assert(
      spark.table(targetTable).schema.fieldNames.toSeq ==
      Seq("id", "name", AutoCdcReservedNames.cdcMetadataColName))
    checkAnswer(
      spark.table(targetTable),
      Seq(
        Row(1L, "alice2", cdcMeta(None, Some(2L))),
        Row(3L, "carol", cdcMeta(None, Some(2L)))))
  }

  test("SCD1: a declared delete condition takes precedence over the changelog's") {
    createScd1Target()
    createChangelogSource("src", insertUpdateDeleteChanges)

    // With a delete condition that never matches, the `delete` of `bob` is applied as an upsert.
    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = F.col("_commit_version"),
      deleteCondition = Some(F.lit(false))
    ))

    checkAnswer(
      spark.table(targetTable),
      Seq(
        Row(1L, "alice2", cdcMeta(None, Some(2L))),
        Row(2L, "bob", cdcMeta(None, Some(3L))),
        Row(3L, "carol", cdcMeta(None, Some(2L)))))
  }

  test("SCD1: a declared include list keeps the changelog metadata columns it names") {
    spark.sql(
      s"CREATE TABLE $targetTable " +
      s"(id BIGINT NOT NULL, name STRING, _commit_version BIGINT, $scd1MetadataDdl)")
    createChangelogSource("src", insertUpdateDeleteChanges)

    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = F.col("_commit_version"),
      columnSelection = Some(ColumnSelection.IncludeColumns(
        Seq("id", "name", "_commit_version").map(UnqualifiedColumnName(_))))
    ))

    checkAnswer(
      spark.table(targetTable),
      Seq(
        Row(1L, "alice2", 2L, cdcMeta(None, Some(2L))),
        Row(3L, "carol", 2L, cdcMeta(None, Some(2L)))))
  }

  test("SCD2: a changelog's commit versions bound each key's history records") {
    spark.sql(s"CREATE TABLE $targetTable (id BIGINT NOT NULL, name STRING, $scd2MetadataDdl)")
    createChangelogSource("src", insertUpdateDeleteChanges)

    runPipeline(singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = F.col("_commit_version"),
      scdType = ScdType.Type2
    ))

    // `alice` has a closed record for the inserted value and an open one for the updated value,
    // `bob` has a record closed by its delete, and `carol` has an open record.
    checkAnswer(
      spark.table(targetTable),
      Seq(
        Row(1L, "alice", 1L, 2L, Row(1L, null)),
        Row(1L, "alice2", 2L, null, Row(2L, null)),
        Row(2L, "bob", 1L, 3L, Row(1L, null)),
        Row(3L, "carol", 2L, null, Row(2L, null))))
  }

  test("SCD1: a changelog read through SQL AUTO CDC ... FROM STREAM ... CHANGES") {
    createScd1Target()
    createChangelogSource("src", insertUpdateDeleteChanges)

    val ctx = new TestGraphRegistrationContext(spark) {
      registerTable("target", catalog = Some(catalog), database = Some(namespace))
    }
    new SqlGraphRegistrationContext(ctx).processSqlFile(
      sqlText = s"""
        |CREATE FLOW auto_cdc_flow AS AUTO CDC INTO $targetTable
        |FROM STREAM $cdcCatalog.$cdcNamespace.src CHANGES FROM VERSION 1
        |KEYS (id)
        |SEQUENCE BY _commit_version
        |""".stripMargin,
      sqlFilePath = "pipeline.sql",
      spark = spark)
    runPipeline(ctx)

    checkAnswer(
      spark.table(targetTable),
      Seq(
        Row(1L, "alice2", cdcMeta(None, Some(2L))),
        Row(3L, "carol", cdcMeta(None, Some(2L)))))
  }

  /** The changes of a connector that emits an update as a `delete` and an `insert` row. */
  private val deleteAndInsertUpdateChanges: Seq[Change] = Seq(
    Change(1L, "alice", CHANGE_TYPE_INSERT, 1L, rowVersion = Some(1L)),
    Change(2L, "bob", CHANGE_TYPE_INSERT, 1L, rowVersion = Some(1L)),
    Change(1L, "alice", CHANGE_TYPE_DELETE, 2L, rowVersion = Some(1L)),
    Change(1L, "alice2", CHANGE_TYPE_INSERT, 2L, rowVersion = Some(2L))
  )

  private val deleteAndInsertUpdateProperties: ChangelogProperties = ChangelogProperties(
    representsUpdateAsDeleteAndInsert = true,
    rowIdNames = Seq("id"),
    rowVersionName = Some("row_version"))

  test("a changelog representing updates as delete and insert must be read with computeUpdates") {
    createScd1Target()
    createChangelogSource("src", deleteAndInsertUpdateChanges, deleteAndInsertUpdateProperties)

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = F.col("_commit_version")
    )

    checkErrorInPipelineFailure(
      failure = intercept[RuntimeException] { runPipeline(ctx) },
      condition = "AUTOCDC_CHANGELOG_SOURCE_NOT_RECONCILABLE.COMPUTE_UPDATES_DISABLED",
      sqlState = Some("22023"),
      parameters = Map(
        "tableName" -> s"`$catalog`.`$namespace`.`target`",
        "changelogName" -> changelogName("src")))
  }

  test("a changelog with carry-over rows must be read with carry-over removal") {
    createScd1Target()
    createChangelogSource(
      "src",
      Seq(Change(1L, "alice", CHANGE_TYPE_INSERT, 1L, rowVersion = Some(1L))),
      ChangelogProperties(
        containsCarryoverRows = true,
        rowIdNames = Seq("id"),
        rowVersionName = Some("row_version")))

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src", Map("deduplicationMode" -> "none")),
      keys = Seq("id"),
      sequencing = F.col("_commit_version")
    )

    checkErrorInPipelineFailure(
      failure = intercept[RuntimeException] { runPipeline(ctx) },
      condition = "AUTOCDC_CHANGELOG_SOURCE_NOT_RECONCILABLE.CARRYOVER_REMOVAL_DISABLED",
      sqlState = Some("22023"),
      parameters = Map(
        "tableName" -> s"`$catalog`.`$namespace`.`target`",
        "changelogName" -> changelogName("src")))
  }

  test("a changelog source must be sequenced by _commit_version") {
    createScd1Target()
    createChangelogSource("src", insertUpdateDeleteChanges)

    val ctx = singleAutoCdcFlowPipeline(
      flowName = "auto_cdc_flow",
      target = "target",
      sourceDf = changelogStream("src"),
      keys = Seq("id"),
      sequencing = F.col("_commit_timestamp")
    )

    checkErrorInPipelineFailure(
      failure = intercept[RuntimeException] { runPipeline(ctx) },
      condition = "AUTOCDC_CHANGELOG_SOURCE_NOT_RECONCILABLE.SEQUENCING_NOT_BY_COMMIT_VERSION",
      sqlState = Some("22023"),
      parameters = Map(
        "tableName" -> s"`$catalog`.`$namespace`.`target`",
        "changelogName" -> changelogName("src"),
        "sequencingExpression" -> "_commit_timestamp"))
  }

  /** The name the in-memory connector gives to the changelog of `cdc.cdcns.<table>`. */
  private def changelogName(table: String): String =
    spark.read
      .option("startingVersion", "1")
      .changes(s"$cdcCatalog.$cdcNamespace.$table")
      .queryExecution
      .analyzed
      .collect { case r: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation =>
        r.table.name()
      }
      .head
}
