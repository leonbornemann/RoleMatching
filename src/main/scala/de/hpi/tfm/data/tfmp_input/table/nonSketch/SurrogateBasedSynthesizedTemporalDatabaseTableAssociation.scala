package de.hpi.tfm.data.tfmp_input.table.nonSketch

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.tfmp_input.association.{AssociationIdentifier, AssociationSchema}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.{getFullTimeRangeFile, getOptimizationInputAssociationFile}
import de.hpi.tfm.data.tfmp_input.table.sketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.tfm.data.tfmp_input.table.{AbstractSurrogateBasedTemporalRow, AbstractSurrogateBasedTemporalTable, TemporalDatabaseTableTrait, TemporalFieldTrait}
import de.hpi.tfm.data.tfmp_input.{BinaryReadable, SynthesizedDatabaseTableRegistry}
import de.hpi.tfm.io.DBSynthesis_IOService
import de.hpi.tfm.io.DBSynthesis_IOService.{OPTIMIZATION_INPUT_ASSOCIATION_DIR, OPTIMIZATION_INPUT_FULL_TIME_RANGE_ASSOCIATION_DIR, createParentDirs}

import java.io.File
import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@SerialVersionUID(3L)
class SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(id:String,
                                                                unionedOriginalTables:mutable.HashSet[AssociationIdentifier],
                                                                key: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                nonKeyAttribute:AttributeLineage,
                                                                foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
                                                                val surrogateBasedTemporalRows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRow] = collection.mutable.ArrayBuffer(),
                                                                uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID())
  extends AbstractSurrogateBasedTemporalTable[Any,SurrogateBasedTemporalRow](id,unionedOriginalTables,key,nonKeyAttribute,foreignKeys,surrogateBasedTemporalRows,uniqueSynthTableID) with Serializable{

  def projectToTimeRange(timeRangeStart: LocalDate, timeRangeEnd: LocalDate): SurrogateBasedSynthesizedTemporalDatabaseTableAssociation = {
    val newRows = rows.map(r => {
      val newFL = r.valueLineage.projectToTimeRange(timeRangeStart,timeRangeEnd)
      buildNewRow(r.keys.head,newFL).asInstanceOf[SurrogateBasedTemporalRow]
    })
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(id,unionedOriginalTables,key,nonKeyAttribute,foreignKeys,newRows)
  }


  def writeToFullTimeRangeFile() = {
    assert(isAssociation && unionedOriginalTables.size==1)
    val file = getFullTimeRangeFile(unionedOriginalTables.head)
    writeToBinaryFile(file)
  }


  def writeToStandardOptimizationInputFile = {
    assert(isAssociation && unionedOriginalTables.size==1)
    val file = getOptimizationInputAssociationFile(unionedOriginalTables.head)
    writeToBinaryFile(file)
  }


  def toSketch = {
    val newRows = surrogateBasedTemporalRows.map(sr => sr.toRowSketch)
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch(id,
      unionedOriginalTables,
      key,
      nonKeyAttribute,
      foreignKeys,
      newRows)
  }


  override def fieldIsWildcardAt(rowIndex: Int, colIndex: Int, ts: LocalDate): Boolean = {
    assert(colIndex==0)
    FactLineage.isWildcard(surrogateBasedTemporalRows(rowIndex).valueLineage.valueAt(ts))
  }

  override def fieldValueAtTimestamp(rowIndex: Int, colIndex: Int, ts: LocalDate): Any = {
    assert(colIndex==0)
    surrogateBasedTemporalRows(rowIndex).valueLineage.valueAt(ts)
  }

  override def isSketch: Boolean = false

  override def createNewTable(unionID: String, unionedTables: mutable.HashSet[Int], value: mutable.HashSet[AssociationIdentifier], key: collection.IndexedSeq[SurrogateAttributeLineage], newNonKEyAttrLineage: AttributeLineage, newRows: ArrayBuffer[AbstractSurrogateBasedTemporalRow[Any]]): TemporalDatabaseTableTrait[Any] = {
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(unionID,
        value,
        key,
        newNonKEyAttrLineage,
        IndexedSeq(),
        newRows.map(_.asInstanceOf[SurrogateBasedTemporalRow]))
  }

  override def wildcardValues = rows.head.valueLineage.WILDCARDVALUES.toSeq

  override def buildNewRow(pk: Int, res: TemporalFieldTrait[Any]): AbstractSurrogateBasedTemporalRow[Any] = {
    new SurrogateBasedTemporalRow(IndexedSeq(pk),res.asInstanceOf[FactLineage],IndexedSeq())
  }

  override def getRow(rowIndex: Int): AbstractSurrogateBasedTemporalRow[Any] = rows(rowIndex)
}

object SurrogateBasedSynthesizedTemporalDatabaseTableAssociation extends
  BinaryReadable[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation] with StrictLogging{

  def loadFomFullTimeRangeFile(id: AssociationIdentifier): SurrogateBasedSynthesizedTemporalDatabaseTableAssociation = {
    val file = getFullTimeRangeFile(id)
    loadFromFile(file)
  }

  def getStandardOptimizationInputFile(id: AssociationIdentifier) = getOptimizationInputAssociationFile(id)

  def loadFromStandardOptimizationInputFile(id:AssociationIdentifier) = {
    val file = getOptimizationInputAssociationFile(id)
    loadFromFile(file)
  }

  def loadAllAssociationTables(associations: IndexedSeq[AssociationSchema]) = {
    associations.map(a => loadFromStandardOptimizationInputFile(a.id))
  }

  def getOptimizationInputAssociationFile(id: AssociationIdentifier) = {
    createParentDirs(new File(s"${OPTIMIZATION_INPUT_ASSOCIATION_DIR(id.subdomain)}/${id.viewID}/${id.compositeID}.binary"))
  }

  def getFullTimeRangeFile(id:AssociationIdentifier) = {
    createParentDirs(new File(s"${OPTIMIZATION_INPUT_FULL_TIME_RANGE_ASSOCIATION_DIR(id.subdomain)}/${id.viewID}/${id.compositeID}.binary"))
  }

  def getOptimizationInputAssociationParentDirs(subdomain:String) = {
    createParentDirs(new File(s"${OPTIMIZATION_INPUT_ASSOCIATION_DIR(subdomain)}/")).listFiles()
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
