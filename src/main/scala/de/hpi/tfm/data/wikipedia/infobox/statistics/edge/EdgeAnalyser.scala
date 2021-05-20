package de.hpi.tfm.data.wikipedia.infobox.statistics.edge

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.data.wikipedia.infobox.statistics.edge
import de.hpi.tfm.evaluation.data.{GeneralEdge, IdentifiedFactLineage}

import java.io.{File, PrintWriter}

class EdgeAnalyser(edges: collection.Seq[GeneralEdge], trainGraphConfig:GraphConfig, TIMESTAMP_RESOLUTION_IN_DAYS:Int,nonInformativeValues:Set[Any]) extends StrictLogging{

  val transitionHistogramForTFIDF:Map[ValueTransition[Any],Int] = GeneralEdge.getTransitionHistogramForTFIDF(edges,TIMESTAMP_RESOLUTION_IN_DAYS)
  val lineageCount:Int = GeneralEdge.getLineageCount(edges)

  def toCSVLine(e: GeneralEdge) = {
    val line = e.toGeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig,nonInformativeValues,transitionHistogramForTFIDF,lineageCount)
    e.printTabularEventLineageString
    println(line.getSchema)
    println(line.toCSVLine)
    line.toCSVLine
  }

  def toCsvFile(f:File): Unit = {
    val pr = new PrintWriter(f)
    pr.println(edges.head.toGeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig,nonInformativeValues,transitionHistogramForTFIDF,lineageCount).getSchema.mkString(","))
    logger.debug(s"Found ${edges.size} edges")
    var done = 0
    edges.foreach(e => {
      pr.println(toCSVLine(e))
      done+=1
      if(done%1000==0)
        logger.debug(s"Done with $done ( ${100*done/edges.size.toDouble}%)")
    })
    pr.close()
  }

}
