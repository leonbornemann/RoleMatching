package de.hpi.tfm.data.tfmp_input.table.sketch

import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.tfmp_input.association.{AssociationIdentifier, AssociationSchema}
import de.hpi.tfm.data.tfmp_input.table.sketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.getOptimizationInputAssociationSketchFile
import de.hpi.tfm.data.tfmp_input.table.{AbstractSurrogateBasedTemporalRow, AbstractSurrogateBasedTemporalTable, TemporalDatabaseTableTrait, TemporalFieldTrait}
import de.hpi.tfm.data.tfmp_input.{BinaryReadable, SynthesizedDatabaseTableRegistry}
import de.hpi.tfm.io.DBSynthesis_IOService
import de.hpi.tfm.io.DBSynthesis_IOService.OPTIMIZATION_INPUT_ASSOCIATION_SKETCH_DIR

import java.io.File
import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(id:String,
                                                                      unionedOriginalTables:mutable.HashSet[AssociationIdentifier],
                                                                      key: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                      nonKeyAttribute:AttributeLineage,
                                                                      foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
                                                                      val surrogateBasedTemporalRowSketches:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRowSketch] = collection.mutable.ArrayBuffer(),
                                                                      uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractSurrogateBasedTemporalTable[Int,SurrogateBasedTemporalRowSketch](id,unionedOriginalTables,key,nonKeyAttribute,foreignKeys,surrogateBasedTemporalRowSketches,uniqueSynthTableID) {

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
    val file = getOptimizationInputAssociationSketchFile(unionedOriginalTables.head)
    writeToBinaryFile(file)
  }

  override def isSketch: Boolean = true

  override def createNewTable(unionID: String, unionedTables: mutable.HashSet[Int], value: mutable.HashSet[AssociationIdentifier], key: collection.IndexedSeq[SurrogateAttributeLineage], newNonKEyAttrLineage: AttributeLineage, newRows: ArrayBuffer[AbstractSurrogateBasedTemporalRow[Int]]): TemporalDatabaseTableTrait[Int] = {
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(unionID,
      value,
      key,
      newNonKEyAttrLineage,
      IndexedSeq(),
      newRows.map(_.asInstanceOf[SurrogateBasedTemporalRowSketch]))
  }

  override def wildcardValues: Seq[Int] = Seq(FactLineageSketch.WILDCARD)

  override def buildNewRow(pk: Int, res: TemporalFieldTrait[Int]): AbstractSurrogateBasedTemporalRow[Int] = {
      new SurrogateBasedTemporalRowSketch(IndexedSeq(pk),res.asInstanceOf[FactLineageSketch],IndexedSeq())
  }

  override def getRow(rowIndex: Int): AbstractSurrogateBasedTemporalRow[Int] = rows(rowIndex)
}
object SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch extends BinaryReadable[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]{

  def getOptimizationInputAssociationSketchFile(id: AssociationIdentifier) = {
    DBSynthesis_IOService.createParentDirs(new File(s"${OPTIMIZATION_INPUT_ASSOCIATION_SKETCH_DIR(id.subdomain)}/${id.viewID}/${id.compositeID}.binary"))
  }

  def getOptimizationInputAssociationSketchParentDirs(subdomain:String) = {
    DBSynthesis_IOService.createParentDirs(new File(s"${OPTIMIZATION_INPUT_ASSOCIATION_SKETCH_DIR(subdomain)}/")).listFiles()
  }

  def getStandardOptimizationInputFile(id: AssociationIdentifier) = getOptimizationInputAssociationSketchFile(id)

  def loadFromStandardOptimizationInputFile(id:AssociationIdentifier):SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch = {
    val file = getOptimizationInputAssociationSketchFile(id)
    loadFromFile(file)
  }

  def loadFromStandardOptimizationInputFile(dtt:AssociationSchema):SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch = {
    loadFromStandardOptimizationInputFile(dtt.id)
  }
}
