package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.{SynthesizedDatabaseTableRegistry, TemporalDatabaseTableTrait}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.db_synthesis.sketches.BinaryReadable
import de.hpi.dataset_versioning.db_synthesis.sketches.column.{TemporalColumnSketch, TemporalColumnTrait}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.Variant2Sketch
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(id:String,
                                                                      unionedTables:mutable.HashSet[Int],
                                                                      unionedOriginalTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                                                      key: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                      nonKeyAttribute:AttributeLineage,
                                                                      foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
                                                                      val surrogateBasedTemporalRowSketches:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRowSketch] = collection.mutable.ArrayBuffer(),
                                                                      uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractSurrogateBasedTemporalTable[Int,SurrogateBasedTemporalRowSketch](id,unionedTables,unionedOriginalTables,key,nonKeyAttribute,foreignKeys,surrogateBasedTemporalRowSketches,uniqueSynthTableID) {

  override def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean = {
    assert(colIndex==0)
    val sketch = surrogateBasedTemporalRowSketches(rowIndex).valueSketch
    sketch.isWildcard(sketch.valueAt(ts))
  }

  override def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): Int = {
    assert(colIndex==0)
    surrogateBasedTemporalRowSketches(rowIndex).valueSketch.valueAt(ts)
  }

  def writeToStandardOptimizationInputFile() = {
    assert(isAssociation && unionedOriginalTables.size==1)
    val file = DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(unionedOriginalTables.head)
    writeToBinaryFile(file)
  }

  override def dataColumns: IndexedSeq[TemporalColumnTrait[Int]] = IndexedSeq(new TemporalColumnSketch(id,nonKeyAttribute,surrogateBasedTemporalRowSketches.map(r => r.valueSketch).toArray))

  override def isSketch: Boolean = true

  override def createNewTable(unionID: String,unionedTables: mutable.HashSet[Int], value: mutable.HashSet[DecomposedTemporalTableIdentifier], key: collection.IndexedSeq[SurrogateAttributeLineage], newNonKEyAttrLineage: AttributeLineage, newRows: ArrayBuffer[AbstractSurrogateBasedTemporalRow[Int]]): TemporalDatabaseTableTrait[Int] = {
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(unionID,
      unionedTables,
      value,
      key,
      newNonKEyAttrLineage,
      IndexedSeq(),
      newRows.map(_.asInstanceOf[SurrogateBasedTemporalRowSketch]))
  }

  override def wildcardValues: Seq[Int] = Seq(Variant2Sketch.WILDCARD)
}
object SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch extends BinaryReadable[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]{

  def loadFromStandardOptimizationInputFile(id:DecomposedTemporalTableIdentifier):SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch = {
    val file = DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(id)
    loadFromFile(file)
  }

  def loadFromStandardOptimizationInputFile(dtt:AssociationSchema):SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch = {
    loadFromStandardOptimizationInputFile(dtt.id)
  }
}
