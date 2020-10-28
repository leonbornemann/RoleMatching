package de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, SurrogateAttributeLineage, TemporalTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.{SynthesizedDatabaseTableRegistry, TemporalDatabaseTableTrait}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{AbstractSurrogateBasedTemporalTable, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.BinaryReadable
import de.hpi.dataset_versioning.db_synthesis.sketches.column.{TemporalColumnSketch, TemporalColumnTrait}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.Variant2Sketch
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable

@SerialVersionUID(3L)
class SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(val id:String,
                                                                      unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                                                      val keys: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                      val nonKeyAttribute:AttributeLineage,
                                                                      val foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
                                                                      val rows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRowSketch] = collection.mutable.ArrayBuffer(),
                                                                      val uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractSurrogateBasedTemporalTable[Int,SurrogateBasedTemporalRowSketch](id,unionedTables,keys,nonKeyAttribute,foreignKeys,rows,uniqueSynthTableID) {

  override def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean = {
    assert(colIndex==0)
    val sketch = rows(rowIndex).valueSketch
    sketch.isWildcard(sketch.valueAt(ts))
  }

  override def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): Int = {
    assert(colIndex==0)
    rows(rowIndex).valueSketch.valueAt(ts)
  }

  def writeToStandardOptimizationInputFile() = {
    assert(isAssociation && unionedTables.size==1)
    val file = DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(unionedTables.head)
    writeToBinaryFile(file)
  }

  override def dataColumns: IndexedSeq[TemporalColumnTrait[Int]] = IndexedSeq(new TemporalColumnSketch(id,nonKeyAttribute,rows.map(r => r.valueSketch).toArray))
}
object SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch extends BinaryReadable[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]{

  def loadFromStandardOptimizationInputFile(id:DecomposedTemporalTableIdentifier):SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch = {
    val file = DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(id)
    loadFromFile(file)
  }

  def loadFromStandardOptimizationInputFile(dtt:SurrogateBasedDecomposedTemporalTable):SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch = {
    loadFromStandardOptimizationInputFile(dtt.id)
  }
}
