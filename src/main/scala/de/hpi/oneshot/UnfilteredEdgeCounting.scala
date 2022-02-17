package de.hpi.oneshot

import de.hpi.util.TableFormatter

import java.time.LocalDate
import scala.io.Source

object UnfilteredEdgeCounting extends App {
  val file = "src/main/resources/tmp/UnfilteredEdges.txt"
  val lines = Source.fromFile(file).getLines().toIndexedSeq

  def extractVariables(configString: String) = {
    val tokens = configString.split("_")
    if(configString.startsWith("NO_DECAY")){
      ("NO_DECAY",None,LocalDate.parse(tokens.last))
    } else {
      ("PROBABILISTIC_DECAY_FUNCTION",Some(tokens(3).toDouble),LocalDate.parse(tokens.last))
    }
  }

  case class UnfilteredEdgeStatRow(configString:String,dsName:String,decayMethod:String,decayProbability:Option[Double],trainTimeEnd:LocalDate,totalCount:Int)

  val rows = lines
    .zipWithIndex
    .filter(_._1.contains("total"))
    .map{case (s,i) => {
      val totalCount = s.split("\\s+")(1).toInt
      val configString = lines(i-1).split("newWikipediaGraphs/")(1).split("/")(0)
      val dsName = lines(i-1).split("/edges/")(0).split("/").last
      val (decayMethod,decayProbability,trainTimeEnd) = extractVariables(configString)
      println(configString,totalCount)
      UnfilteredEdgeStatRow(configString,dsName,decayMethod,decayProbability,trainTimeEnd,totalCount)
    }}
  val sorted = rows
    .sortBy(r => (r.decayMethod,r.dsName,r.decayProbability.getOrElse(0.0),r.trainTimeEnd))
    .map(r => Seq(r.decayMethod,r.dsName,r.decayProbability.getOrElse(Double.NaN),r.trainTimeEnd,r.totalCount))
  private val schema = Seq("decayMethod", "dsName", "decayProbability", "trainTimeEnd", "totalCount")
  TableFormatter.printTable(schema,sorted)
  println(schema.mkString(","))
  sorted.foreach(t => println(t.mkString(",")))
}
