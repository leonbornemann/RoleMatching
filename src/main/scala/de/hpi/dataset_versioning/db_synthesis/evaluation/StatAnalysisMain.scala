package de.hpi.dataset_versioning.db_synthesis.evaluation

import java.io.{File, PrintWriter}
import scala.io.Source

object StatAnalysisMain extends App {
  val statFile = args(0)
  val resDir = args(1)
  val rows = Source.fromFile(statFile)
    .getLines()
    .toIndexedSeq
    .tail
    .map(l => StatRow.from(l))

  def getGroupedAccuracy(groupingKey: (StatRow => Comparable[Any])) = rows
    .groupMap(sr => groupingKey(sr))(sr => if(sr.isValid) 1 else 0)
    .toIndexedSeq
    .sortBy(_._1)
    .map{case (int,validRows) => Seq[Any](int,validRows.sum / validRows.size.toDouble,validRows.size)}

  //does not do any escaping!
  def toCSVFile(file:File,header: Seq[String],values:Seq[Seq[Any]]) = {
    val pr = new PrintWriter(file)
    pr.println(header.mkString(","))
    values.foreach(r => pr.println(r.mkString(",")))
    pr.close()
  }

  val accuracyByNumEqualAtT = getGroupedAccuracy(r => r.numEqualAtT.asInstanceOf[Comparable[Any]])
  val accuracyByNumUnEqualAtT = getGroupedAccuracy(r => r.numNonEqualAtT.asInstanceOf[Comparable[Any]])
  val accuracyByNumOverlappingTransitions = getGroupedAccuracy(r => r.numOverlappingTransitions.asInstanceOf[Comparable[Any]])

  toCSVFile(new File(resDir + "/byNumEqualAtT.csv"),Seq("numEqualAtT","accuracy","support"),accuracyByNumEqualAtT)
  toCSVFile(new File(resDir + "/accuracyByNumUnEqualAtT.csv"),Seq("numUnEqualAtT","accuracy","support"),accuracyByNumUnEqualAtT)
  toCSVFile(new File(resDir + "/accuracyByNumOverlappingTransitions.csv"),Seq("numOverlappingTransitions","accuracy","support"),accuracyByNumOverlappingTransitions)


  //isValid                         bool
  //numNonEqualAtT                 int64
  //numEqualAtT                    int64
  //numOverlappingTransitions      int64
  //MI                           float64
  //entropyReduction             float64
  //dtype: object
  case class StatRow(isValid:Boolean,numNonEqualAtT:Int,numEqualAtT:Int,numOverlappingTransitions:Int,mutualInfo:Double,entropyReduction:Double){

  }

  object StatRow {
    def from(l: String) = {
      val tokens = l.split((","))
      StatRow(tokens(0).toBoolean,
        tokens(1).toInt,
        tokens(2).toInt,
        tokens(3).toInt,
        tokens(4).toDouble,
        tokens(5).toDouble
      )
    }

  }

}
