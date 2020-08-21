package de.hpi.dataset_versioning.db_synthesis.sketches

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalColumn
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

class DecomposedTemporalTableSketch(val tableID:DecomposedTemporalTableIdentifier, val temporalColumnSketches:Array[TemporalColumnSketch]) extends Serializable{

  private def serialVersionUID = 6529685098267757680L

  def tableActiveTimes = temporalColumnSketches.map(_.attributeLineage.activeTimeIntervals).reduce((a,b) => a.union(b))

  def writeToBinaryFile(f:File) = {
    val o = new ObjectOutputStream(new FileOutputStream(f))
    o.writeObject(this)
    o.close()
  }

  def writeToStandardFile() = {
    val temporalTableFile = DBSynthesis_IOService.getDecomposedTemporalTableSketchFile(tableID,temporalColumnSketches.head.fieldLineageSketches.head.getVariantName)
    writeToBinaryFile(temporalTableFile)
  }

}

object DecomposedTemporalTableSketch{

  def loadFromFile(f:File) = {
    val oi = new ObjectInputStream(new FileInputStream(f))
    val sketch = oi.readObject().asInstanceOf[DecomposedTemporalTableSketch]
    oi.close()
    sketch
  }

  def load(tableID:DecomposedTemporalTableIdentifier,variantString:String) = {
    val sketchFile = DBSynthesis_IOService.getDecomposedTemporalTableSketchFile(tableID,variantString)
    loadFromFile(sketchFile)
  }

}
