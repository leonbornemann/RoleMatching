package de.hpi.role_matching.evaluation.tuning

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.ValueTransition
import de.hpi.role_matching.cbrm.evidence_based_weighting.isf.ISFMapStorage

import java.io.{File, PrintWriter}

class EdgeStatisticsGatherer(edges: collection.Seq[SimpleCompatbilityGraphEdge],
                             trainGraphConfig:GraphConfig,
                             TIMESTAMP_RESOLUTION_IN_DAYS:Int,
                             nonInformativeValues:Set[Any],
                             TFIDFMapStorage: Option[ISFMapStorage]) extends StrictLogging{

  val transitionHistogramForTFIDF:Map[ValueTransition,Int] = {
    if(TFIDFMapStorage.isDefined) TFIDFMapStorage.get.asMap else  SimpleCompatbilityGraphEdge.getTransitionHistogramForTFIDF(edges,TIMESTAMP_RESOLUTION_IN_DAYS)
  }
  val lineageCount:Int = transitionHistogramForTFIDF.size

  def toCSVLine(e: SimpleCompatbilityGraphEdge) = {
    val line = e.toGeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig,nonInformativeValues,transitionHistogramForTFIDF,lineageCount)
//    if(!line.remainsValid && line.isInteresting && line.trainMetrics(0) > 0.7 && line.trainMetrics(0) < 0.78 && !line.isNumeric) {
//      e.printTabularEventLineageString
//      println(line.getSchema)
//      println(line.toCSVLine)
//      println()
//    }
    line.toCSVLine
  }

  def toCsvFile(f:File): Unit = {
    val pr = new PrintWriter(f)
    logger.debug(s"Found ${edges.size} edges")
    pr.println(edges.head.toGeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig,nonInformativeValues,transitionHistogramForTFIDF,lineageCount).getSchema.mkString(","))
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
