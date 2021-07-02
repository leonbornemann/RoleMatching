package de.hpi.wikipedia.data.compatiblity_graph

import de.hpi.socrata.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.role_matching.compatibility.GraphConfig
import de.hpi.role_matching.evaluation.edge
import de.hpi.util.CSVUtil

case class WikipediaEdgeStatRow(e: WikipediaInfoboxValueHistoryMatch,TIMESTAMP_RESOLUTION_IN_DAYS:Int,trainGraphConfig:GraphConfig) {
  val (v1,v2) = (e.a,e.b)
  val datasetName = "Wikipedia"
  val v1String = Seq("Wikipedia",v1.template.getOrElse(""),v1.pageID.toString(),v1.key.toString,v1.p).map(CSVUtil.toCleanString(_)).mkString("||")
  val v2String = Seq("Wikipedia",v2.template.getOrElse(""),v2.pageID.toString(),v2.key.toString,v2.p).map(CSVUtil.toCleanString(_)).mkString("||")

  def toGeneralStatRow(nonInformativeValues:Set[Any],
                        transitionHistogramForTFIDF:Map[ValueTransition[Any],Int],
                        lineageCount:Int) =
    edge.GeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig,v1String,v2String,v1.lineage.toFactLineage,v2.lineage.toFactLineage,nonInformativeValues,transitionHistogramForTFIDF,lineageCount)



}
object WikipediaEdgeStatRow{

}
