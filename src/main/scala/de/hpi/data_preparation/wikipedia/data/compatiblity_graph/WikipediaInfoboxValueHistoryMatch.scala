package de.hpi.data_preparation.wikipedia.data.compatiblity_graph

import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch.FactLineage
import de.hpi.socrata.JsonReadable
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.util.TableFormatter
import de.hpi.data_preparation.wikipedia.data.transformed.WikipediaInfoboxValueHistory

case class WikipediaInfoboxValueHistoryMatch(a: WikipediaInfoboxValueHistory, b: WikipediaInfoboxValueHistory) extends JsonWritable[WikipediaInfoboxValueHistoryMatch]{
  def toGeneralEdge: SimpleCompatbilityGraphEdge = simple.SimpleCompatbilityGraphEdge(a.toIdentifiedFactLineage,b.toIdentifiedFactLineage)


  def printTabularEventLineageString = {
    val id1 = a.toWikipediaURLInfo
    val id2 = b.toWikipediaURLInfo
    val dates = a.lineage.toFactLineage.lineage.keySet//.filter(v => !FactLineage.isWildcard(v._2) && v._2!="").keySet
    val dates2 = b.lineage.toFactLineage.lineage.keySet//.filter(v => !FactLineage.isWildcard(v._2) && v._2!="").keySet
    val allDates = dates.union(dates2).toIndexedSeq.sorted
    val header = Seq("") ++ allDates
    val cells1 = Seq(id1) ++ allDates.map(t => a.lineage.toFactLineage.valueAt(t)).map(v => if(FactLineage.isWildcard(v)) "_" else v)
    val cells2 = Seq(id2) ++ allDates.map(t => b.lineage.toFactLineage.valueAt(t)).map(v => if(FactLineage.isWildcard(v)) "_" else v)
    TableFormatter.printTable(header,Seq(cells1,cells2))
  }

  def toWikipediaEdgeStatRow(graphConfig: GraphConfig, timestampResolutionInDays: Int) =
    WikipediaEdgeStatRow(this,timestampResolutionInDays,graphConfig)


}

object WikipediaInfoboxValueHistoryMatch extends JsonReadable[WikipediaInfoboxValueHistoryMatch]{

}
