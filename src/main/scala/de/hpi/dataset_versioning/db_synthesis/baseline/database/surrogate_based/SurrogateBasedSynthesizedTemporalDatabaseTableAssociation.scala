package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SynthesizedTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.SynthesizedTemporalDatabaseTable.loadFromFile
import de.hpi.dataset_versioning.db_synthesis.baseline.database.{SynthesizedDatabaseTableRegistry, TemporalDatabaseTableTrait}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch
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
                                                                unionedTables:mutable.HashSet[Int],
                                                                unionedOriginalTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                                                key: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                nonKeyAttribute:AttributeLineage,
                                                                foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
                                                                val surrogateBasedTemporalRows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRow] = collection.mutable.ArrayBuffer(),
                                                                uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractSurrogateBasedTemporalTable[Any,SurrogateBasedTemporalRow](id,unionedTables,unionedOriginalTables,key,nonKeyAttribute,foreignKeys,surrogateBasedTemporalRows,uniqueSynthTableID) with Serializable{

  def writeToStandardOptimizationInputFile = {
    assert(isAssociation && unionedOriginalTables.size==1)
    val file = DBSynthesis_IOService.getOptimizationInputAssociationFile(unionedOriginalTables.head)
    writeToBinaryFile(file)
  }


  def toSketch = {
    val newRows = surrogateBasedTemporalRows.map(sr => sr.toRowSketch)
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(id,
      unionedTables,
      unionedOriginalTables,
      key,
      nonKeyAttribute,
      foreignKeys,
      newRows)
  }


  override def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean = {
    assert(colIndex==0)
    ValueLineage.isWildcard(surrogateBasedTemporalRows(rowIndex).valueLineage.valueAt(ts))
  }

  override def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): Any = {
    assert(colIndex==0)
    surrogateBasedTemporalRows(rowIndex).valueLineage.valueAt(ts)
  }

  override def dataColumns: IndexedSeq[TemporalColumnTrait[Any]] = IndexedSeq(new SurrogateBasedTemporalColumn(nonKeyAttribute,surrogateBasedTemporalRows))

  override def isSketch: Boolean = false

  override def createNewTable(unionID: String,unionedTables: mutable.HashSet[Int], value: mutable.HashSet[DecomposedTemporalTableIdentifier], key: collection.IndexedSeq[SurrogateAttributeLineage], newNonKEyAttrLineage: AttributeLineage, newRows: ArrayBuffer[AbstractSurrogateBasedTemporalRow[Any]]): TemporalDatabaseTableTrait[Any] = {
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(unionID,
        unionedTables,
        value,
        key,
        newNonKEyAttrLineage,
        IndexedSeq(),
        newRows.map(_.asInstanceOf[SurrogateBasedTemporalRow]))
  }
}

object SurrogateBasedSynthesizedTemporalDatabaseTableAssociation extends
  BinaryReadable[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation] with StrictLogging{

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
