package de.hpi.dataset_versioning.entropy

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.DatasetMetadata
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{AssociationGraphEdge, DataBasedMatchCalculator, TupleReference}
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
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
    .filter(a => a.firstMatchPartner.viewID != a.secondMatchPartner.viewID)
  val explorer = new AssociationGraphExplorer()

  def inspect(edges: collection.Seq[AssociationGraphEdge]) = {
    val targetDir = new File(resultDir)
    edges.foreach(e => {
      val firstMatchPartner = e.firstMatchPartner
      val secondMatchPartner = e.secondMatchPartner
      val pr = new PrintWriter(targetDir.getAbsolutePath + "/" + firstMatchPartner.compositeID + "_U_" + secondMatchPartner.compositeID)
      explorer.printInfoToFile(Some(e), firstMatchPartner, secondMatchPartner, pr)
      pr.close()
    })
  }

  def translateToInts(sequence: IndexedSeq[Any], temporalFieldTrait: TemporalFieldTrait[Any]) = {
    var curChar: Int = 1
    val mapping = mutable.HashMap[Any, Int]()
    val chars = sequence.map(c => {
      if (temporalFieldTrait.isWildcard(c))
        0
      else if (mapping.contains(c))
        mapping(c)
      else {
        mapping.put(c, curChar.toChar)
        curChar += 1
        (curChar - 1)
      }
    })
    chars
  }

  def getAsFlatString(value: TupleReference[Any], map: collection.Map[Any, Char]) = {
    val lineage = value.getDataTuple.head
    val sequence = (IOService.STANDARD_TIME_FRAME_START.toEpochDay to IOService.STANDARD_TIME_FRAME_END.toEpochDay).map(l => {
      map(lineage.valueAt(LocalDate.ofEpochDay(l)))
    })
    sequence.mkString
  }

  def getTranslationMap(tupleReferences: Seq[TupleReference[Any]]) = {
    val allValuesSortedByFirstOccurrence = tupleReferences.flatMap(tr => tr.getDataTuple.head.getValueLineage)
      .groupBy(_._2)
      .map { case (k, v) => (k, v.minBy(_._1.toEpochDay)._1) }
      .toSeq
      .sortBy(_._2.toEpochDay)
      .map(_._1)
      .filter(v => v != ReservedChangeValues.NOT_EXISTANT_DATASET && v != ReservedChangeValues.NOT_EXISTANT_COL && ReservedChangeValues.NOT_EXISTANT_ROW != v)
    var curCharIndex: Int = 0
    val symbols = (65 to 90) ++ (97 to 122) ++ (192 to 214) ++ (223 to 246) ++ (256 to 328) ++ (330 to 447) ++ (452 to 591)
    val mapping = mutable.HashMap[Any, Char]()
    mapping.put(ReservedChangeValues.NOT_EXISTANT_COL, '_')
    mapping.put(ReservedChangeValues.NOT_EXISTANT_DATASET, '_')
    mapping.put(ReservedChangeValues.NOT_EXISTANT_ROW, '_')
    allValuesSortedByFirstOccurrence.foreach(c => {
      if (!mapping.contains(c)) {
        mapping.put(c, symbols(curCharIndex).toChar)
        curCharIndex += 1
      }
    })
    mapping
  }

  def serializeEdges(edges: collection.Seq[AssociationGraphEdge]) = {
    val pr = new PrintWriter(resultDir + "/" + "edgeContentsAsStrings.csv")
    pr.println("edge1_id,edge1_tupleIndices,edge2-id,edge2_tupleIndex,edgesValues")
    edges
      .foreach(e => {
        println(s"processing ${e}")
        val a1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.firstMatchPartner)
        val a2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.secondMatchPartner)
        val unionMatch = new DataBasedMatchCalculator().calculateMatch(a1, a2, true)
        val edgeValuesAsStrings = unionMatch.tupleMapping.get.matchedTuples
          .filter(_.tupleReferences.size > 1)
          .foreach(m => {
            val map = getTranslationMap(m.tupleReferences)
            val tupleValues = m.tupleReferences.map(getAsFlatString(_, map))
            val tupleIndices1 = m.tupleReferences.filter(_.table == a1).map(_.rowIndex)
            val tupleIndices2 = m.tupleReferences.filter(_.table == a2).map(_.rowIndex)
            val toPrint = s"${e.firstMatchPartner},${tupleIndices1.mkString(";")},${e.secondMatchPartner},${tupleIndices2.mkString(";")},${tupleValues.mkString(";")}"
            pr.println(toPrint)
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
