package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.AssociationGraphEdge
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import java.io.{File, PrintWriter}

object AssociationGraphEdgeExplorationMain extends App {
  IOService.socrataDir = args(0)
  val resultDir = args(1)
  val edges = AssociationGraphEdge.fromJsonObjectPerLineFile(DBSynthesis_IOService.getAssociationGraphEdgeFile.getAbsolutePath)
  val sum = edges.map(_.minChangeBenefit).sum
  println(sum)
  println()
  val edgesBetweenDifferentViewTableColumns = edges.sortBy(-_.evidence)
    .filter(a => a.firstMatchPartner.viewID!=a.secondMatchPartner.viewID)

  val versionHistory = DatasetVersionHistory.loadAsMap()

  def loadMetadata(firstMatchPartner: DecomposedTemporalTableIdentifier) = {
    val lastVersion = versionHistory(firstMatchPartner.viewID).versionsWithChanges.last
    IOService.cacheMetadata(lastVersion)
    IOService.cachedMetadata(lastVersion)(firstMatchPartner.viewID)
  }

  def getIfInRange(seq: Seq[Any],supposedSize:Int, pos: Int) = if(pos>=seq.size) "OutOfBounds" else if(supposedSize!=seq.size) "sizes don't match" else seq(pos)

  def printMetaInfo(association: DecomposedTemporalTableIdentifier, metadata: DatasetMetadata, pr: PrintWriter) = {
    pr.println(s"------------------------------${association.viewID}----------------------------------")
    pr.println(s"${metadata.resource.name}")
    pr.println(s"${metadata.resource.description}")
    val associationSchema1 = AssociationSchema.load(association)
    val attrPosTry1 = metadata.resource.columns_field_name.indexOf(associationSchema1.attributeLineage.lastName)
    val attrPosTry2 = metadata.resource.columns_name.indexOf(associationSchema1.attributeLineage.lastName)
    if(attrPosTry1 == -1 && attrPosTry2== -1)
      pr.println("No column info because name is not found")
    else {
      val pos = if(attrPosTry1 != -1) metadata.resource.columns_field_name.size else metadata.resource.columns_name.size
      val nEntries = if(attrPosTry1 != -1) attrPosTry1 else attrPosTry2
      pr.println(s"Column Display Name: ${getIfInRange(metadata.resource.columns_name,nEntries,pos)}")
      pr.println(s"Column Field Name: ${getIfInRange(metadata.resource.columns_field_name,nEntries,pos)}")
      pr.println(s"Column Format: ${getIfInRange(metadata.resource.columns_format,nEntries,pos)}")
      pr.println(s"Column Datatype: ${getIfInRange(metadata.resource.columns_dataytpe,nEntries,pos)}")
      pr.println(s"Column Description: ${getIfInRange(metadata.resource.columns_description,nEntries,pos)}")
    }
    pr.println(s"-----------------------------------------------------------------------------------")
    pr.println(s"-----------------------------------------------------------------------------------")
  }

  def inspect(edges: collection.Seq[AssociationGraphEdge]) = {
    val targetDir = new File(resultDir)
    edges.foreach( e => {
      val pr = new PrintWriter(targetDir.getAbsolutePath + "/" + e.firstMatchPartner.compositeID + "_U_" + e.secondMatchPartner.compositeID)
      val associationSchema1 = AssociationSchema.load(e.firstMatchPartner)
      val associationSchema2 = AssociationSchema.load(e.secondMatchPartner)
      pr.println(s"First: ${e.firstMatchPartner}, attribute: ${associationSchema1.attributeLineage.nameSet}")
      pr.println(s"Second: ${e.secondMatchPartner}, attribute: ${associationSchema2.attributeLineage.nameSet}")
      pr.println(s"${e.toJson()}")
      val metadataView1 = loadMetadata(e.firstMatchPartner)
      val metadataView2 = loadMetadata(e.secondMatchPartner)
      printMetaInfo(e.firstMatchPartner,metadataView1,pr)
      printMetaInfo(e.secondMatchPartner,metadataView2,pr)
      pr.close()
    })
  }

  inspect(edgesBetweenDifferentViewTableColumns.take(100))
//  println(edgesBetweenDifferentViewTableColumns.size)
//  println()
//  edgesBetweenDifferentViewTableColumns
//    .take(100)
//    .map(age => {
//      val table1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(age.firstMatchPartner)
//      val table2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(age.secondMatchPartner)
//      (table1.nonKeyAttribute.nameSet,table2.nonKeyAttribute.nameSet)
//    })
//    .foreach(println(_))
//  val sameColNameEdges = edges.filter(e => {
//    val table1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.firstMatchPartner)
//    val table2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.secondMatchPartner)
//    (table1.nonKeyAttribute.nameSet==table2.nonKeyAttribute.nameSet)
//  })
//  println(sameColNameEdges.size)
//  edgesBetweenDifferentViewTableColumns.foreach(println(_))
//  print(edgesBetweenDifferentViewTableColumns.size)
}
