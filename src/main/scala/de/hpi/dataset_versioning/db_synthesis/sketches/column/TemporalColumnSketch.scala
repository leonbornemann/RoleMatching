package de.hpi.dataset_versioning.db_synthesis.sketches.column

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalColumn
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.{FieldLineageSketch, TemporalFieldTrait, Variant2Sketch}
import de.hpi.dataset_versioning.db_synthesis.sketches.{BinaryReadable, BinarySerializable}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

@SerialVersionUID(3L)
class TemporalColumnSketch(val tableID:String, val attributeLineage:AttributeLineage, val fieldLineageSketches:Array[FieldLineageSketch]) extends TemporalColumnTrait[Int] with BinarySerializable{
  def attrID = attributeLineage.attrId

  override def fieldLineages: IndexedSeq[TemporalFieldTrait[Int]] = fieldLineageSketches


}

object TemporalColumnSketch extends BinaryReadable[TemporalColumnSketch]{

  def loadAll(id: String,variantString:String) = {
    val sketchDir = DBSynthesis_IOService.getTemporalColumnSketchDir(id)
    sketchDir.listFiles()
      .filter(f => f.getName.contains(variantString))
      .map(f => loadFromFile(f))
  }

  def from(tc: TemporalColumn) = {
    new TemporalColumnSketch(tc.id,tc.attributeLineage,tc.lineages.map(el => Variant2Sketch.fromValueLineage(el.lineage)).toArray)
  }

  def load(tableID:String,attrID:Int,variantString:String) = {
    val sketchFile = DBSynthesis_IOService.getTemporalColumnSketchFile(tableID,attrID,variantString)
    loadFromFile(sketchFile)
  }
}
