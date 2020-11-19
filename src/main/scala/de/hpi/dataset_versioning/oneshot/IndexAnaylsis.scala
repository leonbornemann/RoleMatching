package de.hpi.dataset_versioning.oneshot

import java.time.LocalDate

import scala.io.Source

object IndexAnaylsis extends App {
  val lines = Source.fromFile("indexStats.csv")
    .getLines()
    .toIndexedSeq
    .tail
    .map(s => IndexNodeReport.from(s) )
  //histograms:
//  val numTableHistogram = lines
//    .groupBy(_.numTuples)
//    .map(t => (t._1,t._2.size))
//    .toIndexedSeq
//    .sortBy(_._1)
//    .foreach(t => println(s"${t._1}:${t._2}"))

  //xy-plots:
  val xyPlot = lines
    .filter(_.numTuples_Rank_2>0)
    .map(t => (t.numTuples_Rank_1,t.numTuples_Rank_2))
  xyPlot
    //.foreach(t => println(t._2))
    .foreach(t => println(s"${t._1}, ${t._2}"))


  case class IndexNodeReport(nodeID:Int,
                             timestamps:Seq[LocalDate],
                             nodeValues:Seq[Any],
                             numTables:Int,
                             numTuples:Int,
                             numTuples_Rank_1:Int,
                             numTuples_Rank_2:Int,
                             numTuples_Rank_3:Int,
                             numTuples_Rank_4:Int,
                             numTuples_Rank_5:Int)

  object IndexNodeReport {
    def from(s:String) = {
      val tokens = s.split(",")
      IndexNodeReport(tokens(0).toInt,
      tokens(1).split(";").map(s => LocalDate.parse(s)),
      tokens(2).split(";"),
      tokens(3).toInt,
        tokens(4).toInt,
        tokens(5).toInt,
        tokens(6).toInt,
        tokens(7).toInt,
        tokens(8).toInt,
        tokens(8).toInt)
    }
  }

}
