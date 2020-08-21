package de.hpi.dataset_versioning.db_synthesis.sketches

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, TemporalColumn}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

class TemporalColumnSketch(val tableID:String, val attributeLineage:AttributeLineage, val fieldLineageSketches:Array[FieldLineageSketch]) extends Serializable{
  def attrID = attributeLineage.attrId


  private def serialVersionUID = 6529685098267757690L

  def writeToBinaryFile(f:File) = {
    val o = new ObjectOutputStream(new FileOutputStream(f))
    o.writeObject(this)
    o.close()
  }
}

object TemporalColumnSketch {

  def loadAll(id: String,variantString:String) = {
    val sketchDir = DBSynthesis_IOService.getTemporalColumnSketchDir(id)
    sketchDir.listFiles()
      .filter(f => f.getName.contains(variantString))
      .map(f => loadFromFile(f))
  }


  def from(tc: TemporalColumn) = {
    new TemporalColumnSketch(tc.id,tc.attributeLineage,tc.lineages.map(el => Variant2Sketch.fromValueLineage(el.lineage)).toArray)
  }

  def loadFromFile(f:File) = {
    val oi = new ObjectInputStream(new FileInputStream(f))
    val sketch = oi.readObject().asInstanceOf[TemporalColumnSketch]
    oi.close()
    sketch
  }

  def load(tableID:String,attrID:Int,variantString:String) = {
    val sketchFile = DBSynthesis_IOService.getTemporalColumnSketchFile(tableID,attrID,variantString)
    loadFromFile(sketchFile)
  }
}
