package de.hpi.role_matching.evaluation.exploration

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.graph.representation.slim.VertexLookupMap
import de.hpi.role_matching.compatibility.graph.representation.vertex.IdentifiedFactLineage
import de.hpi.role_matching.scoring.MultipleEventWeightScoreComputer
import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage

import java.io.PrintWriter
import java.time.LocalDate
import scala.collection.mutable
import scala.io.Source

object RenameMatchFinder extends App {

  GLOBAL_CONFIG.setDatesForDataSource("wikipedia")
  private val dsName = "education"
  val csvFile ="/home/leon/data/dataset_versioning/plotting/exportedData/truePositiveEdges/" + dsName + ".csv"
  val vertexLookupMap = VertexLookupMap.fromJsonFile("/home/leon/data/dataset_versioning/vertexLookupMaps/" + dsName + ".json")
  val resultFile = "/home/leon/data/dataset_versioning/outdatedExploration/" + dsName + ".txt"
  val pr = new PrintWriter(resultFile)
  val seqWithName = vertexLookupMap.posToLineage.values.toIndexedSeq.map(idfl => (idfl.csvSafeID,(idfl,idfl.factLineage.toFactLineage))).toMap
  val edges = Source.fromFile(csvFile)
    .getLines()
    .toIndexedSeq
    .tail
    .map(l => {
      val tokens = l.split(",")
      val id1 = tokens(1)
      val id2 = tokens(2)
      (id1,id2)
    })

  def twoYearDifference(a: LocalDate, b: LocalDate) = {
    a.plusYears(2).isBefore(b) || b.plusYears(2).isBefore(a)
  }

  def getPageID(str: String) = {
    val res = str.split("\\|\\|")(1)
    res
  }

  def isOutdated(date: LocalDate) = {
    date.plusYears(2).isBefore(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END)
  }

  def oneIsOutdated(lineage: FactLineage, lineage1: FactLineage) = {
    val value1 = lineage.lineage.toIndexedSeq.reverse.find(t => !FactLineage.isWildcard(t._2)).get
    val value2 = lineage1.lineage.toIndexedSeq.reverse.find(t => !FactLineage.isWildcard(t._2)).get
    val res = (isOutdated(value1._1) && !isOutdated(value2._1) || isOutdated(value2._1) && !isOutdated(value1._1) ) && value1._2 != value2._2 && !GLOBAL_CONFIG.nonInformativeValues.contains(value1._2) && !GLOBAL_CONFIG.nonInformativeValues.contains(value2._2)
    if(res) {
      res
    }
    res
  }

  def getScore(l1: FactLineage, l2: FactLineage) = {
//    MultipleEventWeightScoreComputer[A](a:TemporalFieldTrait[A],
//      b:TemporalFieldTrait[A],
//    val TIMESTAMP_GRANULARITY_IN_DAYS:Int,
//    timeEnd:LocalDate, // this should be the end of train time!
//    nonInformativeValues:Set[A],
//    nonInformativeValueIsStrict:Boolean, //true if it is enough for one value in a transition to be non-informative to discard it, false if both of them need to be non-informative to discard it
//    transitionHistogramForTFIDF:Option[Map[ValueTransition[A],Int]],
//    lineageCount:Option[Int],
//    tfidfWeightingOption:Option[TFIDFWeightingVariant]
//    ) {
      val computer = new MultipleEventWeightScoreComputer(l1,
        l2,
        7,
        GLOBAL_CONFIG.STANDARD_TIME_FRAME_END,
        GLOBAL_CONFIG.nonInformativeValues,
        true,
        None,
        None,
        None
      )
      computer.score()
  }

  edges
    .withFilter(e => {
      val (lineage1,l1) = seqWithName(e._1)
      val (lineage2,l2) = seqWithName(e._2)
      val nonWildcard1 = l1.lineage.find(t => !FactLineage.isWildcard(t._2)).get
      val nonWildcard2 = l2.lineage.find(t => !FactLineage.isWildcard(t._2)).get
      /*oneIsOutdated(l1,l2) &&*/ highChangeCount(l1) && highChangeCount(l2) && !e._1.contains("image") && !e._2.contains("image") //&& e._1.contains("honorific-prefix") && e._2.contains("honorific-prefix")
      //twoYearDifference(nonWildcard1._1,nonWildcard2._1) && getPageID(e._1) == getPageID(e._2)
    })
    .map(e => {
      val (lineage1,l1) = seqWithName(e._1)
      val (lineage2,l2) = seqWithName(e._2)
      val str = IdentifiedFactLineage.getTabularEventLineageString(Seq(lineage1,lineage2))
      val score = getScore(l1,l2)
      (str,score)
    })
    .sortBy(-_._2)
    .foreach(pr.println(_))

  private def highChangeCount(l1: FactLineage) = {
    l1.lineage.values.toSet.size > 7
  }

  pr.close()
}
