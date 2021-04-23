package de.hpi.tfm.data.wikipedia.infobox.query

import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory

object QueryAnalysis extends App {
  val vertexFile = args(0)
  val edgeFile = args(1)
  val edges = WikipediaInfoboxValueHistoryMatch.fromJsonObjectPerLineFile(edgeFile)
  val ids = Set(BigInt(24113),BigInt(32212))
  println(edges.filter(e => ids.forall(id => Set(e.a.pageID,e.b.pageID).contains(id))))
  val vertices = WikipediaInfoboxValueHistory.fromJsonObjectPerLineFile(vertexFile)
  val president = vertices.filter(v => v.pageID==BigInt(24113) && v.p=="incumbent_\uD83D\uDD17_extractedLink0").head
  val armedForces = vertices.filter(v => v.pageID==BigInt(32212) && v.p=="commander-in-chief_\uD83D\uDD17_extractedLink1").head
  val res = president.lineage.toFactLineage.tryMergeWithConsistent(armedForces.lineage.toFactLineage)
  println()
}
