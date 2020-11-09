package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.logger
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.sketches.BinarySerializable
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
abstract class AbstractSurrogateBasedTemporalTable[A,B <: AbstractSurrogateBasedTemporalRow[A]](val id: String,
                                                                                                val unionedTables: mutable.HashSet[Int],
                                                                                                val unionedOriginalTables: mutable.HashSet[DecomposedTemporalTableIdentifier],
                                                                                                val key: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                                                val nonKeyAttribute: AttributeLineage,
                                                                                                val foreignKeys: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                                                val rows:collection.mutable.ArrayBuffer[B],
                                                                                                val uniqueSynthTableID: Int) extends  TemporalDatabaseTableTrait[A] with BinarySerializable with StrictLogging with Serializable{

  override def toString: String = unionedOriginalTables.mkString("_UNION_")+ s"(${key.mkString(",")}, ${nonKeyAttribute})"

  def isSketch: Boolean

  override def insertTime: LocalDate = rows
    .flatMap(_.value.getValueLineage
      .filter(t => !ValueLineage.isWildcard(t._2))
      .map(_._1))
    .minBy(_.toEpochDay)

  def writeToStandardTemporaryFile() = {
    val f = DBSynthesis_IOService.getSynthesizedTableTempFile(uniqueSynthTableID)
    writeToBinaryFile(f)
  }

  override def getDataTuple(rowIndex: Int): collection.IndexedSeq[TemporalFieldTrait[A]] = IndexedSeq(rows(rowIndex).value)

  override def primaryKeyIsValid: Boolean = ???

  override def informativeTableName: String = getID + "(" + key.mkString(",") + ",  " + nonKeyAttribute.lastName + ",  " + foreignKeys.mkString(",") + ")"

  override def nrows: Int = rows.size

  override def getID: String = id

  override def getUnionedOriginalTables: collection.Set[DecomposedTemporalTableIdentifier] = unionedOriginalTables

  override def primaryKey: collection.Set[AttributeLineage] = ???

  override def dataAttributeLineages: collection.IndexedSeq[AttributeLineage] = IndexedSeq(nonKeyAttribute)

  override def isSurrogateBased: Boolean = true

  def createNewTable(unionID: String,unionedTables: mutable.HashSet[Int], value: mutable.HashSet[DecomposedTemporalTableIdentifier], key: collection.IndexedSeq[SurrogateAttributeLineage], newNonKEyAttrLineage: AttributeLineage, newRows: ArrayBuffer[AbstractSurrogateBasedTemporalRow[A]]):TemporalDatabaseTableTrait[A]

  def createUnionedTable(left: AbstractSurrogateBasedTemporalTable[A, B], right: AbstractSurrogateBasedTemporalTable[A, B], bestMatch: TableUnionMatch[A]) = {
    val unionID = left.getID + "_UNION_" + right.getID
    val newNonKEyAttrLineage:AttributeLineage = left.nonKeyAttribute.union(right.nonKeyAttribute,left.nonKeyAttribute.attrId)
    var curSurrogateKeyCounter = left.rows.maxBy(_.keys.head).keys.head +1
    val nonMatchedRowsRight = bestMatch.tupleMapping.get.unmatchedTupleIndicesB.toIndexedSeq.map(rowIndex => {
      val r = right.rows(rowIndex).asInstanceOf[AbstractSurrogateBasedTemporalRow[A]]
      assert(r.keys.size==1 && r.foreignKeys.size==0)
      val newRow = r.cloneWithNewKey(curSurrogateKeyCounter)
      curSurrogateKeyCounter +=1
      newRow
    })
    val nonMatchedLeftRows =  bestMatch.tupleMapping.get.unmatchedTupleIndicesA.toIndexedSeq.map(rowIndex => {
      left.rows(rowIndex).asInstanceOf[AbstractSurrogateBasedTemporalRow[A]]
    })
    val mergedRows = bestMatch.tupleMapping.get.matchedTuples.map(tm => {
      val leftRow = left.rows(tm.tupleIndexA).asInstanceOf[AbstractSurrogateBasedTemporalRow[A]]
      val rightRow = right.rows(tm.tupleIndexB).asInstanceOf[AbstractSurrogateBasedTemporalRow[A]]
      leftRow.mergeWithConsistent(leftRow.keys,rightRow)
    })
    val newRows = mutable.ArrayBuffer() ++ (nonMatchedLeftRows ++ mergedRows ++ nonMatchedRowsRight).sortBy(_.keys.head)
    assert(left.foreignKeys.isEmpty && right.foreignKeys.isEmpty)
    var newTable:AbstractSurrogateBasedTemporalTable[A, B] = null
    createNewTable(unionID,
      mutable.HashSet(left.uniqueSynthTableID,right.uniqueSynthTableID),
      mutable.HashSet() ++ left.getUnionedOriginalTables.union(right.getUnionedOriginalTables),
      left.key,
      newNonKEyAttrLineage,
      newRows
    )
  }

  override def executeUnion(other: TemporalDatabaseTableTrait[A], bestMatch: TableUnionMatch[A]): TemporalDatabaseTableTrait[A] = {
    var left = this.asInstanceOf[AbstractSurrogateBasedTemporalTable[A,B]]
    var right = other.asInstanceOf[AbstractSurrogateBasedTemporalTable[A,B]]
    if(bestMatch.firstMatchPartner==right){
      left = other.asInstanceOf[AbstractSurrogateBasedTemporalTable[A,B]]
      right = this.asInstanceOf[AbstractSurrogateBasedTemporalTable[A,B]]
    }
    assert(left == bestMatch.firstMatchPartner && right == bestMatch.secondMatchPartner)
    val a = createUnionedTable(left,right,bestMatch)
    a
  }
}
