package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{AssociationGraphEdge, DataBasedMatchCalculator, TupleReference}
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable

object AssociationGraphEdgeExplorationMain extends App {
  IOService.socrataDir = args(0)
  val resultDir = args(1)
  val edges = AssociationGraphEdge.fromJsonObjectPerLineFile(DBSynthesis_IOService.getAssociationGraphEdgeFile.getAbsolutePath)
    .sortBy(-_.evidence)
  val sum = edges.map(_.minChangeBenefit).sum
  println(sum)
  println()
  val edgesBetweenDifferentViewTableColumns = edges
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
      val a1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.firstMatchPartner)
      val a2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.secondMatchPartner)
      pr.println(s"#Tuples(${e.firstMatchPartner}): ${a1.nrows}")
      pr.println(s"#Tuples(${e.secondMatchPartner}): ${a2.nrows}")
      val unionMatch = new DataBasedMatchCalculator().calculateMatch(a1,a2,true)
      unionMatch.tupleMapping.get.totalEvidence
      pr.println(s"True Evidence: ${unionMatch.tupleMapping.get.totalEvidence}")
      pr.println(s"True Change Benefit: ${unionMatch.tupleMapping.get.totalChangeBenefit}")
      pr.println("-----------------------------------------Matches:------------------------------------------")
      var matchCount = 0
      unionMatch.tupleMapping.get.matchedTuples
        .filter(_.evidence>0)
        .foreach(tm => {
          pr.println(s"--------------------------------------------------Match $matchCount (EVIDENCE ${tm.evidence}, change improvement: ${tm.changeRange})------------------------------------------------")
          pr.println("First Relation")
          val byTAble = tm.tupleReferences.groupBy(_.table)
          byTAble.getOrElse(a1,Seq()).foreach(tr => pr.println(tr.getDataTuple.head.getValueLineage))
          pr.println("Second Relation")
          byTAble.getOrElse(a2,Seq()).foreach(tr => pr.println(tr.getDataTuple.head.getValueLineage))
          pr.println(s"-------------------------------------------------------------------------------------------------------------------")
          matchCount+=1
      })
      pr.close()
    })
  }

  def translateToInts(sequence: IndexedSeq[Any]) = {
    var curChar:Int = 0
    val mapping = mutable.HashMap[Any,Int]()
    val chars = sequence.map(c => {
      if(mapping.contains(c))
        mapping(c)
      else {
        mapping.put(c,curChar.toChar)
        curChar +=1
        (curChar-1)
      }
    })
    chars
  }

  def getAsFlatString(value: TupleReference[Any]) = {
    val lineage = value.getDataTuple.head
    val sequence = (IOService.STANDARD_TIME_FRAME_START.toEpochDay to IOService.STANDARD_TIME_FRAME_END.toEpochDay).map(l => {
      lineage.valueAt(LocalDate.ofEpochDay(l))
    })
    val ints = translateToInts(sequence)
    val start = 'A'
    val end = 'Z'
    ints.map(i => {
      val second = start + i % (end-start)
      val first = start + i / (end-start)
      Seq(first.toChar,second.toChar).mkString
    }).mkString
  }

  def serializeEdges(edges: collection.Seq[AssociationGraphEdge]) = {
    val pr = new PrintWriter(resultDir + "/" + "edgeContentsAsStrings.csv")
    pr.println("edge1_id,edge1_tupleIndices,edge2-id,edge2_tupleIndex,edgesValues")
    edges.foreach( e => {
      println(s"processing ${e}")
      val a1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.firstMatchPartner)
      val a2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.secondMatchPartner)
      val unionMatch = new DataBasedMatchCalculator().calculateMatch(a1,a2,true)
      val edgeValuesAsStrings = unionMatch.tupleMapping.get.matchedTuples.foreach(m => {
        val tupleValues = m.tupleReferences.map(getAsFlatString(_))
        val tupleIndices1 = m.tupleReferences.filter(_.table==a1).map(_.rowIndex)
        val tupleIndices2 = m.tupleReferences.filter(_.table==a2).map(_.rowIndex)
        pr.println(e.firstMatchPartner,tupleIndices1.mkString(";"),e.secondMatchPartner,tupleIndices2.mkString(";"),tupleValues.mkString(";"))
      })
      pr.flush()
    })
    pr.close()
  }

  serializeEdges(edges)
  //inspect(edgesBetweenDifferentViewTableColumns.take(100))
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
