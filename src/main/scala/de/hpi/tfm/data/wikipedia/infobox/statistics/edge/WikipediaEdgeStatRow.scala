package de.hpi.tfm.data.wikipedia.infobox.statistics.edge

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.evaluation.wikipediaStyle.GeneralEdgeStatRow
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScore
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.{RuzickaSimilarityWildcardIgnore, TransitionMatchScore}
import de.hpi.tfm.util.CSVUtil

import java.time.LocalDate

case class WikipediaEdgeStatRow(e: WikipediaInfoboxValueHistoryMatch,TIMESTAMP_RESOLUTION_IN_DAYS:Int,trainGraphConfig:GraphConfig) {
  val (v1,v2) = (e.a,e.b)
  val datasetName = "Wikipedia"
  val v1String = Seq("Wikipedia",v1.template.getOrElse(""),v1.pageID.toString(),v1.key.toString,v1.p).map(CSVUtil.toCleanString(_)).mkString("||")
  val v2String = Seq("Wikipedia",v2.template.getOrElse(""),v2.pageID.toString(),v2.key.toString,v2.p).map(CSVUtil.toCleanString(_)).mkString("||")

  def toGeneralStatRow = GeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig,v1String,v2String,v1.lineage.toFactLineage,v2.lineage.toFactLineage)



}
object WikipediaEdgeStatRow{

}
