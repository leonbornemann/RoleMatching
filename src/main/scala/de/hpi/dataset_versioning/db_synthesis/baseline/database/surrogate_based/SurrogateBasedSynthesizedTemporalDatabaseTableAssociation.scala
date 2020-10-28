package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import java.time.LocalDate

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
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.{BinaryReadable, BinarySerializable}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable

@SerialVersionUID(3L)
class SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(val id:String,
                                                                unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                                                val keys: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                val nonKeyAttribute:AttributeLineage,
                                                                val foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
                                                                val rows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRow] = collection.mutable.ArrayBuffer(),
                                                                val uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractSurrogateBasedTemporalTable[Any,SurrogateBasedTemporalRow](id,unionedTables,keys,nonKeyAttribute,foreignKeys,rows,uniqueSynthTableID) with Serializable{

  def writeToStandardOptimizationInputFile = {
    assert(isAssociation && unionedTables.size==1)
    val file = DBSynthesis_IOService.getOptimizationInputAssociationFile(unionedTables.head)
    writeToBinaryFile(file)
  }


  def toSketch = {
    val newRows = rows.map(sr => sr.toRowSketch)
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(id,
      unionedTables,
      keys,
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

}

object SurrogateBasedSynthesizedTemporalDatabaseTableAssociation extends BinaryReadable[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]{

  def loadFromStandardFile(id: Int):SurrogateBasedSynthesizedTemporalDatabaseTableAssociation = {
    loadFromFile(DBSynthesis_IOService.getSynthesizedTableTempFile(id))
  }

  def loadFromStandardOptimizationInputFile(id:DecomposedTemporalTableIdentifier) = {
    val file = DBSynthesis_IOService.getOptimizationInputAssociationFile(id)
    loadFromFile(file)
  }

  def loadAllAssociationTables(associations: IndexedSeq[SurrogateBasedDecomposedTemporalTable]) = {
    associations
      .groupBy(_.id.viewID)
      .flatMap{case (viewID,g) => {
        val tt = TemporalTable.load(viewID)
        g.map(a => SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.initFrom(a,tt))
      }}
      .toIndexedSeq
  }

  def initFrom(dttToMerge: SurrogateBasedDecomposedTemporalTable, originalTemporalTable:TemporalTable) = {
    assert(dttToMerge.attributes.size==1)
    val tt = originalTemporalTable.project(dttToMerge)
    val keys = dttToMerge.surrogateKey
    val foreignKeys = dttToMerge.foreignSurrogateKeysToReferencedTables.map(_._1)
    val pkSurrogateOrder = dttToMerge.surrogateKey.map(sl => tt.projection.surrogateAttributes.indexOf(sl))
    val fkSurrogateOrder = dttToMerge.foreignSurrogateKeysToReferencedTables.map(_._1).map(sl => tt.projection.surrogateAttributes.indexOf(sl))
    val rows = mutable.ArrayBuffer() ++ (0 until tt.projection.rows.size).map(rowIndex => {
      val data = tt.projection.rows(rowIndex).fields
      assert(data.size==1)
      val surrogateRow = tt.projection.surrogateRows(rowIndex)
      val pk = pkSurrogateOrder.map(i => surrogateRow(i))
      val fk = fkSurrogateOrder.map(i => surrogateRow(i))
      new SurrogateBasedTemporalRow(pk,data.head,fk)
    })
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(dttToMerge.compositeID,
      mutable.HashSet(dttToMerge.id),
      keys,
      dttToMerge.attributes.head,
      foreignKeys,
      rows
    )
  }

}
