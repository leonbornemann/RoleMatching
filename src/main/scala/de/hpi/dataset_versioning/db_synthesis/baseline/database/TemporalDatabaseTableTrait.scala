package de.hpi.dataset_versioning.db_synthesis.baseline.database

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

trait TemporalDatabaseTableTrait[A] {
  def wildcardValues:Seq[A]


  def insertTime:LocalDate

  def getDataTuple(rowIndex: Int): collection.IndexedSeq[TemporalFieldTrait[A]]

  def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean

  def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): A

  def isAssociation: Boolean = dataAttributeLineages.size == 1

  def isTrueUnion = getUnionedOriginalTables.size > 1

  def primaryKeyIsValid: Boolean

  def informativeTableName: String

  def nrows: Int

  def getID: String

  def getUnionedOriginalTables: collection.Set[DecomposedTemporalTableIdentifier]

  def dataColumns: IndexedSeq[TemporalColumnTrait[A]]

  def primaryKey: collection.Set[AttributeLineage]

  def dataAttributeLineages: collection.IndexedSeq[AttributeLineage]

  def isSurrogateBased:Boolean

  def executeUnion(other: TemporalDatabaseTableTrait[A], bestMatch: TableUnionMatch[A]): TemporalDatabaseTableTrait[A]

}
