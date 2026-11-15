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

package org.apache.spark.sql.pipelines.autocdc

import org.apache.spark.sql.{functions => F, AnalysisException, Column}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.{Changelog, ChangelogContext}
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * A [[Changelog]] read feeding an AutoCDC flow, with the parts of the changelog contract that
 * AutoCDC needs to complete the flow's [[ChangeArgs]] and to validate the read.
 *
 * A changelog read (`DataStreamReader.changes` or `STREAM ... CHANGES`) emits one row per change,
 * tagged with the `_change_type`, `_commit_version` and `_commit_timestamp` metadata columns. The
 * [[Changelog]] contract fixes what these columns mean, so AutoCDC can take its CDC semantics from
 * them instead of asking the user to map them by hand:
 *  - a row is a delete iff `_change_type` is `delete`;
 *  - `update_preimage` rows carry the value an update replaced and must not be applied; the
 *    `update_postimage` row at the same `_commit_version` carries the new value;
 *  - the metadata columns describe the change, not the row, and are not part of the target.
 *
 * @param changelogName                     The connector's name for the changelog, for messages.
 * @param changeTypeColumnName              `_change_type` as spelled in the flow's output schema.
 * @param metadataColumnNames               The changelog metadata columns present in the flow's
 *                                          output schema, spelled as they appear there.
 * @param representsUpdateAsDeleteAndInsert Whether the connector emits an update as a raw `delete`
 *                                          and `insert` pair, see
 *                                          `Changelog.representsUpdateAsDeleteAndInsert()`.
 * @param containsCarryoverRows             Whether the connector may emit copy-on-write carry-over
 *                                          pairs, see `Changelog.containsCarryoverRows()`.
 * @param computeUpdates                    Whether the read asked Spark to turn `delete` and
 *                                          `insert` pairs into update pre- and post-images.
 * @param deduplicationMode                 The read's deduplication mode.
 */
case class ChangelogSource(
    changelogName: String,
    changeTypeColumnName: String,
    metadataColumnNames: Seq[String],
    representsUpdateAsDeleteAndInsert: Boolean,
    containsCarryoverRows: Boolean,
    computeUpdates: Boolean,
    deduplicationMode: ChangelogContext.DeduplicationMode) {

  private def changeTypeColumn: Column = F.col(QuotingUtils.quoteIdentifier(changeTypeColumnName))

  /**
   * Whether the read still emits an update as a `delete` and an `insert` row at the same
   * `_commit_version`, i.e. the connector represents updates that way and the read did not ask
   * Spark to detect them.
   */
  def updatesArriveAsDeleteAndInsert: Boolean = representsUpdateAsDeleteAndInsert && !computeUpdates

  /**
   * Whether the read still emits copy-on-write carry-over pairs, i.e. the connector may emit them
   * and the read disabled their removal.
   */
  def carryoversRetained: Boolean =
    containsCarryoverRows && deduplicationMode == ChangelogContext.DeduplicationMode.NONE

  /**
   * Rejects a read that AutoCDC cannot reconcile deterministically.
   *
   * AutoCDC orders the events of a key by the sequencing expression and keeps the greatest one per
   * microbatch. It therefore needs that, among the rows it applies, no two rows of the same key
   * share a sequence value while meaning different things. A changelog read breaks this in three
   * ways, each rejected here:
   *  - updates arriving as a `delete` and an `insert` row at the same `_commit_version`: with
   *    `computeUpdates` the pair becomes an `update_preimage` and an `update_postimage` row, and
   *    the pre-image is filtered out by [[withoutUpdatePreimages]];
   *  - copy-on-write carry-over pairs: an unchanged row is re-emitted as an identical `insert` and
   *    `delete` at the same `_commit_version`, and the `delete` half looks like a real delete;
   *  - a sequencing expression that does not reference `_commit_version`: the [[Changelog]]
   *    contract only promises that `_commit_version` orders changes in commit order. In
   *    particular `_commit_timestamp` is shared by every row of a commit and is only strictly
   *    increasing across micro-batches, so it cannot order two changes to the same key.
   *
   * @param sequencing            The flow's declared sequencing expression.
   * @param changeDataFeed        The flow's resolved source dataframe, against which `sequencing`
   *                              is resolved.
   * @param destinationIdentifier The flow's target table, for messages.
   * @param resolver              The flow's column name resolver.
   */
  def validate(
      sequencing: Column,
      changeDataFeed: DataFrame,
      destinationIdentifier: TableIdentifier,
      resolver: Resolver): Unit = {
    def fail(subCondition: String, extraParameters: (String, String)*): Nothing = {
      throw new AnalysisException(
        errorClass = s"AUTOCDC_CHANGELOG_SOURCE_NOT_RECONCILABLE.$subCondition",
        messageParameters = Map(
          "tableName" -> destinationIdentifier.quotedString,
          "changelogName" -> changelogName
        ) ++ extraParameters
      )
    }
    if (updatesArriveAsDeleteAndInsert) {
      fail("COMPUTE_UPDATES_DISABLED")
    }
    if (carryoversRetained) {
      fail("CARRYOVER_REMOVAL_DISABLED")
    }
    val sequencingReferences =
      changeDataFeed.select(sequencing).queryExecution.analyzed.references.map(_.name)
    if (!sequencingReferences.exists(resolver(_, ChangelogSource.commitVersionColumnName))) {
      fail("SEQUENCING_NOT_BY_COMMIT_VERSION", "sequencingExpression" -> sequencing.toString)
    }
  }

  /**
   * Completes the declared [[ChangeArgs]] from the changelog contract.
   *
   * The delete condition becomes `_change_type = 'delete'` unless the user declared one, which
   * wins. The metadata columns are added to the excluded columns unless the user declared an
   * explicit include list, which is honored as written: a user who lists `_commit_version` wants
   * it in the target.
   */
  def completeChangeArgs(declared: ChangeArgs, resolver: Resolver): ChangeArgs = {
    val deleteCondition =
      declared.deleteCondition.orElse(Some(changeTypeColumn === Changelog.CHANGE_TYPE_DELETE))
    val metadataColumns = metadataColumnNames.map(name => UnqualifiedColumnName(Seq(name)))
    val columnSelection = declared.columnSelection match {
      case None =>
        Some(ColumnSelection.ExcludeColumns(metadataColumns))
      case Some(ColumnSelection.ExcludeColumns(excluded)) =>
        val alreadyExcluded = (c: UnqualifiedColumnName) =>
          excluded.exists(e => resolver(e.name, c.name))
        Some(ColumnSelection.ExcludeColumns(excluded ++ metadataColumns.filterNot(alreadyExcluded)))
      case included @ Some(ColumnSelection.IncludeColumns(_)) =>
        included
    }
    declared.copy(deleteCondition = deleteCondition, columnSelection = columnSelection)
  }

  /**
   * Drops the `update_preimage` rows from a change data feed. A pre-image shares its key and
   * `_commit_version` with the post-image that replaces it, so applying both would leave the
   * winner between them to chance. Rows with a null `_change_type` are kept, like any other row
   * the delete condition does not classify as a delete.
   */
  def withoutUpdatePreimages(changeDataFeed: DataFrame): DataFrame =
    changeDataFeed.filter(!changeTypeColumn.eqNullSafe(Changelog.CHANGE_TYPE_UPDATE_PREIMAGE))
}

object ChangelogSource {
  /** The [[Changelog]] metadata column holding the kind of change. */
  val changeTypeColumnName: String = "_change_type"

  /** The [[Changelog]] metadata column holding the commit that emitted the change. */
  val commitVersionColumnName: String = "_commit_version"

  /** The [[Changelog]] metadata column holding the timestamp of that commit. */
  val commitTimestampColumnName: String = "_commit_timestamp"

  /** All [[Changelog]] metadata columns. */
  val metadataColumnNames: Seq[String] =
    Seq(changeTypeColumnName, commitVersionColumnName, commitTimestampColumnName)

  /**
   * Detects whether a flow's resolved source is a changelog read whose change metadata survives
   * into the flow's output, and if so describes it.
   *
   * A source qualifies when its analyzed plan reads exactly one changelog, the read being
   * resolved to a [[ChangelogTable]] under a [[StreamingRelationV2]] (streaming) or a
   * [[DataSourceV2Relation]] (batch), and its output schema still carries the `_change_type` and
   * `_commit_version` columns, each exactly once under `resolver`. A query that projects the
   * change metadata away, renames it, or joins several changelogs is not a changelog source: the
   * contract no longer tells how its rows map to CDC events, so the flow keeps its declared
   * [[ChangeArgs]] as is.
   */
  def detect(changeDataFeed: DataFrame, resolver: Resolver): Option[ChangelogSource] = {
    val changelogTables = changeDataFeed.queryExecution.analyzed.collect {
      case DataSourceV2Relation(table: ChangelogTable, _, _, _, _, _) => table
      case StreamingRelationV2(_, _, table: ChangelogTable, _, _, _, _, _, _) => table
    }
    changelogTables match {
      case Seq(table) =>
        val schema = changeDataFeed.schema
        for {
          changeTypeField <- uniqueField(schema, changeTypeColumnName, resolver)
          if changeTypeField.dataType == StringType
          _ <- uniqueField(schema, commitVersionColumnName, resolver)
        } yield {
          ChangelogSource(
            changelogName = table.changelog.name(),
            changeTypeColumnName = changeTypeField.name,
            metadataColumnNames =
              metadataColumnNames.flatMap(uniqueField(schema, _, resolver)).map(_.name),
            representsUpdateAsDeleteAndInsert = table.changelog.representsUpdateAsDeleteAndInsert(),
            containsCarryoverRows = table.changelog.containsCarryoverRows(),
            computeUpdates = table.changelogContext.computeUpdates(),
            deduplicationMode = table.changelogContext.deduplicationMode()
          )
        }
      case _ => None
    }
  }

  /** The single field of `schema` named `name` under `resolver`, if there is exactly one. */
  private def uniqueField(
      schema: StructType,
      name: String,
      resolver: Resolver): Option[StructField] = {
    schema.fields.filter(field => resolver(field.name, name)) match {
      case Array(field) => Some(field)
      case _ => None
    }
  }
}
