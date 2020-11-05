package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, SurrogateAttributeLineage, TemporalTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.loadFromFile
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch, SurrogateBasedTemporalRowSketch, SynthesizedTemporalDatabaseTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SynthesizedTemporalDatabaseTable.loadFromFile
import de.hpi.dataset_versioning.db_synthesis.baseline.database.{SynthesizedDatabaseTableRegistry, TemporalDatabaseTableTrait}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.database.GlobalSurrogateRegistry
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.{BinaryReadable, BinarySerializable}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(id:String,
                                                                unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                                                key: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                nonKeyAttribute:AttributeLineage,
                                                                foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
                                                                rows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRow] = collection.mutable.ArrayBuffer(),
                                                                val uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractSurrogateBasedTemporalTable[Any,SurrogateBasedTemporalRow](id,unionedTables,key,nonKeyAttribute,foreignKeys,rows,uniqueSynthTableID) with Serializable{

  def writeToStandardOptimizationInputFile = {
    assert(isAssociation && unionedTables.size==1)
    val file = DBSynthesis_IOService.getOptimizationInputAssociationFile(unionedTables.head)
    writeToBinaryFile(file)
  }


  def toSketch = {
    val newRows = rows.map(sr => sr.toRowSketch)
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(id,
      unionedTables,
      key,
      nonKeyAttribute,
      foreignKeys,
      newRows)
  }


  override def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean = {
    assert(colIndex==0)
    ValueLineage.isWildcard(rows(rowIndex).valueLineage.valueAt(ts))
  }

  override def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): Any = {
    assert(colIndex==0)
    rows(rowIndex).valueLineage.valueAt(ts)
  }

  override def dataColumns: IndexedSeq[TemporalColumnTrait[Any]] = IndexedSeq(new SurrogateBasedTemporalColumn(nonKeyAttribute,rows))

  override def isSketch: Boolean = false

  override def createNewTable(unionID: String, value: mutable.HashSet[DecomposedTemporalTableIdentifier], key: collection.IndexedSeq[SurrogateAttributeLineage], newNonKEyAttrLineage: AttributeLineage, newRows: ArrayBuffer[AbstractSurrogateBasedTemporalRow[Any]]): TemporalDatabaseTableTrait[Any] = {
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(unionID,
        value,
        key,
        newNonKEyAttrLineage,
        IndexedSeq(),
        newRows.map(_.asInstanceOf[SurrogateBasedTemporalRow]))
  }
}

object SurrogateBasedSynthesizedTemporalDatabaseTableAssociation extends
  BinaryReadable[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation] with StrictLogging{

  def createUnionedTable[A](left: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, right: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, bestMatch: TableUnionMatch[A]) = {
    val unionID = left.getID + "_UNION_" + right.getID
    val newNonKEyAttrLineage:AttributeLineage = left.nonKeyAttribute.unionDisjoint(right.nonKeyAttribute,left.nonKeyAttribute.attrId)
    logger.debug("Remember that the non-key attribute id is now the id of the left table - this change needs to be registered somewhere in the database")
    var curSurrogateKeyCounter = left.rows.maxBy(_.keys.head).keys.head +1
    val nonMatchedRowsRight = bestMatch.tupleMapping.get.unmatchedTupleIndicesB.toIndexedSeq.map(rowIndex => {
      val r = right.rows(rowIndex)
      assert(r.keys.size==1 && r.foreignKeys.size==0)
      val newRow = new SurrogateBasedTemporalRow(IndexedSeq(curSurrogateKeyCounter),r.valueLineage,IndexedSeq())
      curSurrogateKeyCounter +=1
      newRow
    })
    val nonMatchedLeftRows =  bestMatch.tupleMapping.get.unmatchedTupleIndicesA.toIndexedSeq.map(rowIndex => {
      left.rows(rowIndex)
    })
    val mergedRows = bestMatch.tupleMapping.get.matchedTuples.map(tm => {
      val leftRow = left.rows(tm.tupleIndexA)
      val rightRow = right.rows(tm.tupleIndexB)
      val mergedValueLineage = leftRow.valueLineage.mergeWithConsistent(rightRow.valueLineage)
      new SurrogateBasedTemporalRow(leftRow.keys,mergedValueLineage,IndexedSeq())
    })
    val newRows = mutable.ArrayBuffer() ++ (nonMatchedLeftRows ++ mergedRows ++ nonMatchedRowsRight).sortBy(_.keys.head)
    assert(left.foreignKeys.isEmpty && right.foreignKeys.isEmpty)
    val newTable = new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(unionID,
      mutable.HashSet() ++ left.getUnionedTables.union(right.getUnionedTables),
      left.key,
      newNonKEyAttrLineage,
      IndexedSeq(),
      newRows)
    newTable
  }


  def loadFromSynthDatabaseTableFile(id: Int):SurrogateBasedSynthesizedTemporalDatabaseTableAssociation = {
    loadFromFile(DBSynthesis_IOService.getSynthesizedTableTempFile(id))
  }

  def loadFromStandardOptimizationInputFile(id:DecomposedTemporalTableIdentifier) = {
    val file = DBSynthesis_IOService.getOptimizationInputAssociationFile(id)
    loadFromFile(file)
  }

  def loadAllAssociationTables(associations: IndexedSeq[AssociationSchema]) = {
    associations.map(a => loadFromStandardOptimizationInputFile(a.id))
  }

//  def initFrom(dttToMerge: AssociationSchema, originalTemporalTable:TemporalTable) = {
//    if(!originalTemporalTable.hasSurrogateValues){
//      originalTemporalTable.addSurrogates(Set(dttToMerge.surrogateKey))
//    }
//    val tt = originalTemporalTable.project(dttToMerge)
//    val keys = dttToMerge.surrogateKey
//    val foreignKeys = dttToMerge.foreignSurrogateKeysToReferencedTables.map(_._1)
//    val pkSurrogateOrder = dttToMerge.surrogateKey.map(sl => tt.projection.surrogateAttributes.indexOf(sl))
//    val fkSurrogateOrder = dttToMerge.foreignSurrogateKeysToReferencedTables.map(_._1).map(sl => tt.projection.surrogateAttributes.indexOf(sl))
//    val rows = mutable.ArrayBuffer() ++ (0 until tt.projection.rows.size).map(rowIndex => {
//      val data = tt.projection.rows(rowIndex).fields
//      assert(data.size==1)
//      val surrogateRow = tt.projection.surrogateRows(rowIndex)
//      val pk = pkSurrogateOrder.map(i => surrogateRow(i))
//      val fk = fkSurrogateOrder.map(i => surrogateRow(i))
//      new SurrogateBasedTemporalRow(pk,data.head,fk)
//    })
//    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(dttToMerge.compositeID,
//      mutable.HashSet(dttToMerge.id),
//      keys,
//      dttToMerge.attributes.head,
//      foreignKeys,
//      rows
//    )
//  }

}
