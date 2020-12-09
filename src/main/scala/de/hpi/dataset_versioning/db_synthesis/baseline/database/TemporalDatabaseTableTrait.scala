package de.hpi.dataset_versioning.db_synthesis.baseline.database

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.AbstractSurrogateBasedTemporalRow
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait TemporalDatabaseTableTrait[A] {

  def createNewTable(unionID: String,unionedTables: mutable.HashSet[Int], value: mutable.HashSet[DecomposedTemporalTableIdentifier], key: collection.IndexedSeq[SurrogateAttributeLineage], newNonKEyAttrLineage: AttributeLineage, newRows: ArrayBuffer[AbstractSurrogateBasedTemporalRow[A]]):TemporalDatabaseTableTrait[A]

  def getKey:collection.IndexedSeq[SurrogateAttributeLineage]

  def getUniqueSynthTableID: Int

  def getForeignKeys:collection.IndexedSeq[SurrogateAttributeLineage]

  def buildNewRow(curSurrogateKeyCounter: Int, res: TemporalFieldTrait[A]): AbstractSurrogateBasedTemporalRow[A]

  def getNonKeyAttribute:AttributeLineage

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
