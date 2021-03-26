package de.hpi.tfm.data.tfmp_input.table

import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait TemporalDatabaseTableTrait[A] {
  def getRow(rowIndex: Int): AbstractSurrogateBasedTemporalRow[A]

  def nonWildcardValueTransitions = (0 until nrows).toSet.flatMap((i: Int) => {
    val transitionsInTuple: Set[ValueTransition[A]] = getDataTuple(i).head.valueTransitions()
    transitionsInTuple
  })

  def createNewTable(unionID: String, unionedTables: mutable.HashSet[Int], value: mutable.HashSet[AssociationIdentifier], key: collection.IndexedSeq[SurrogateAttributeLineage], newNonKEyAttrLineage: AttributeLineage, newRows: ArrayBuffer[AbstractSurrogateBasedTemporalRow[A]]): TemporalDatabaseTableTrait[A]

  def getKey: collection.IndexedSeq[SurrogateAttributeLineage]

  def getUniqueSynthTableID: Int

  def getForeignKeys: collection.IndexedSeq[SurrogateAttributeLineage]

  def buildNewRow(curSurrogateKeyCounter: Int, res: TemporalFieldTrait[A]): AbstractSurrogateBasedTemporalRow[A]

  def getNonKeyAttribute: AttributeLineage

  def wildcardValues: Seq[A]

  def insertTime: LocalDate

  def getDataTuple(rowIndex: Int): collection.IndexedSeq[TemporalFieldTrait[A]]

  def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean

  def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): A

  def isAssociation: Boolean = dataAttributeLineages.size == 1

  def isTrueUnion = getUnionedOriginalTables.size > 1

  def primaryKeyIsValid: Boolean

  def informativeTableName: String

  def nrows: Int

  def getID: String

  def getUnionedOriginalTables: collection.Set[AssociationIdentifier]

  def primaryKey: collection.Set[AttributeLineage]

  def dataAttributeLineages: collection.IndexedSeq[AttributeLineage]

  def isSurrogateBased: Boolean

}
