package de.hpi.dataset_versioning.db_synthesis.sketches

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalColumn
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

class DecomposedTemporalTableSketch(val tableID:DecomposedTemporalTableIdentifier, temporalColumnSketches:Array[TemporalColumnSketch]) extends TemporalTableSketch(temporalColumnSketches) with Serializable{

  def writeToStandardFile() = {
    val temporalTableFile = DBSynthesis_IOService.getDecomposedTemporalTableSketchFile(tableID,temporalColumnSketches.head.fieldLineageSketches.head.getVariantName)
    writeToBinaryFile(temporalTableFile)
  }

}

object DecomposedTemporalTableSketch{

  def load(tableID:DecomposedTemporalTableIdentifier,variantString:String):DecomposedTemporalTableSketch = {
    val sketchFile = DBSynthesis_IOService.getDecomposedTemporalTableSketchFile(tableID,variantString)
    TemporalTableSketch.loadFromFile[DecomposedTemporalTableSketch](sketchFile)
  }

  def load(tableID:DecomposedTemporalTableIdentifier):DecomposedTemporalTableSketch = {
    load(tableID,Variant2Sketch.getVariantName)
  }

}
