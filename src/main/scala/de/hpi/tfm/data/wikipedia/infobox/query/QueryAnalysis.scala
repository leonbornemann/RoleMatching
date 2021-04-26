package de.hpi.tfm.data.wikipedia.infobox.query

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.wikipedia.infobox.original.InfoboxRevisionHistory
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory
import de.hpi.tfm.io.IOService

import java.io.File

object QueryAnalysis extends App with StrictLogging{
  val vertexFile = args(0)
  val edgeFile = args(1)
  val edgeResultFile = args(2)
  val edges = WikipediaInfoboxValueHistoryMatch.fromJsonObjectPerLineFile(edgeFile)
    .zipWithIndex
    .filter(_._2>5000)
    .map(_._1)
  val edgeAnalyser = new EdgeAnalyser(edges)
  val vertices = WikipediaInfoboxValueHistory.fromJsonObjectPerLineFile(vertexFile)
  logger.debug(s"Loaded query result with ${vertices.size} vertices and ${edges.size} edges")
  IOService.STANDARD_TIME_FRAME_START = InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP
  IOService.STANDARD_TIME_FRAME_END = InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP
  edgeAnalyser.toCsvFile(new File(edgeResultFile))
  assert(false) //TODO: delete filter at top
//  //President.incumbent and ArmedForces.commander in chief:
//  val ids = Set(BigInt(24113),BigInt(32212))
//  println(edges.filter(e => ids.forall(id => Set(e.a.pageID,e.b.pageID).contains(id))))
//  val president = vertices.filter(v => v.pageID==BigInt(24113) && v.p=="incumbent_\uD83D\uDD17_extractedLink0").head
//  val armedForces = vertices.filter(v => v.pageID==BigInt(32212) && v.p=="commander-in-chief_\uD83D\uDD17_extractedLink1").head
//  println("President")
//  president.lineage.toFactLineage.lineage.foreach(println(_))
//  println("Armed Forces")
//  armedForces.lineage.toFactLineage.lineage.foreach(println(_))
//  val res = president.lineage.toFactLineage.tryMergeWithConsistent(armedForces.lineage.toFactLineage)
//  println()
}
