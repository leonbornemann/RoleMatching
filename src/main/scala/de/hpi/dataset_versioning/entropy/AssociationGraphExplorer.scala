package de.hpi.dataset_versioning.entropy

import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{AssociationGraphEdge, DataBasedMatchCalculator}
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.IOService

import java.io.PrintWriter

class AssociationGraphExplorer {

  val versionHistory = DatasetVersionHistory.loadAsMap()

  def loadMetadata(firstMatchPartner: DecomposedTemporalTableIdentifier) = {
    val lastVersion = versionHistory(firstMatchPartner.viewID).versionsWithChanges.last
    IOService.cacheMetadata(lastVersion)
    IOService.cachedMetadata(lastVersion)(firstMatchPartner.viewID)
  }

  def getIfInRange(seq: Seq[Any], supposedSize: Int, pos: Int) = if (pos >= seq.size) "OutOfBounds" else if (supposedSize != seq.size) "sizes don't match" else seq(pos)

  def printMetaInfo(association: DecomposedTemporalTableIdentifier, metadata: DatasetMetadata, pr: PrintWriter) = {
    pr.println(s"------------------------------${association.viewID}----------------------------------")
    pr.println(s"${metadata.resource.name}")
    pr.println(s"${metadata.resource.description}")
    val associationSchema1 = AssociationSchema.load(association)
    val attrPosTry1 = metadata.resource.columns_field_name.indexOf(associationSchema1.attributeLineage.lastName)
    val attrPosTry2 = metadata.resource.columns_name.indexOf(associationSchema1.attributeLineage.lastName)
    if (attrPosTry1 == -1 && attrPosTry2 == -1)
      pr.println("No column info because name is not found")
    else {
      val pos = if (attrPosTry1 != -1) metadata.resource.columns_field_name.size else metadata.resource.columns_name.size
      val nEntries = if (attrPosTry1 != -1) attrPosTry1 else attrPosTry2
      pr.println(s"Column Display Name: ${getIfInRange(metadata.resource.columns_name, nEntries, pos)}")
      pr.println(s"Column Field Name: ${getIfInRange(metadata.resource.columns_field_name, nEntries, pos)}")
      pr.println(s"Column Format: ${getIfInRange(metadata.resource.columns_format, nEntries, pos)}")
      pr.println(s"Column Datatype: ${getIfInRange(metadata.resource.columns_dataytpe, nEntries, pos)}")
      pr.println(s"Column Description: ${getIfInRange(metadata.resource.columns_description, nEntries, pos)}")
    }
    pr.println(s"-----------------------------------------------------------------------------------")
    pr.println(s"-----------------------------------------------------------------------------------")
  }

  def printInfoToFile(e: Option[AssociationGraphEdge],
                              firstMatchPartner: DecomposedTemporalTableIdentifier,
                              secondMatchPartner: DecomposedTemporalTableIdentifier,
                              pr: PrintWriter) = {
    val associationSchema1 = AssociationSchema.load(firstMatchPartner)
    val associationSchema2 = AssociationSchema.load(secondMatchPartner)
    pr.println(s"First: $firstMatchPartner, attribute: ${associationSchema1.attributeLineage.nameSet}")
    pr.println(s"Second: $secondMatchPartner, attribute: ${associationSchema2.attributeLineage.nameSet}")
    if (e.isDefined)
      pr.println(s"${e.get.toJson()}")
    val metadataView1 = loadMetadata(firstMatchPartner)
    val metadataView2 = loadMetadata(secondMatchPartner)
    printMetaInfo(firstMatchPartner, metadataView1, pr)
    printMetaInfo(secondMatchPartner, metadataView2, pr)
    val a1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(firstMatchPartner)
    val a2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(secondMatchPartner)
    pr.println(s"#Tuples($firstMatchPartner): ${a1.nrows}")
    pr.println(s"#Tuples($secondMatchPartner): ${a2.nrows}")
    val unionMatch = new DataBasedMatchCalculator().calculateMatch(a1, a2, true)
    unionMatch.tupleMapping.get.totalEvidence
    pr.println(s"True Evidence: ${unionMatch.tupleMapping.get.totalEvidence}")
    pr.println(s"True Change Benefit: ${unionMatch.tupleMapping.get.totalChangeBenefit}")
    pr.println("-----------------------------------------Matches:------------------------------------------")
    var matchCount = 0
    unionMatch.tupleMapping.get.matchedTuples
      .filter(_.evidence > 0)
      .foreach(tm => {
        pr.println(s"--------------------------------------------------Match $matchCount (EVIDENCE ${tm.evidence}, change improvement: ${tm.changeRange})------------------------------------------------")
        pr.println("First Relation")
        val byTAble = tm.tupleReferences.groupBy(_.table)
        byTAble.getOrElse(a1, Seq()).foreach(tr => pr.println(tr.getDataTuple.head.getValueLineage))
        pr.println("Second Relation")
        byTAble.getOrElse(a2, Seq()).foreach(tr => pr.println(tr.getDataTuple.head.getValueLineage))
        pr.println(s"-------------------------------------------------------------------------------------------------------------------")
        matchCount += 1
      })
  }

}
