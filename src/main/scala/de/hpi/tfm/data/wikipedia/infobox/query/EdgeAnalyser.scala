package de.hpi.tfm.data.wikipedia.infobox.query

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.data.wikipedia.infobox.query.QueryAnalysis.edges
import de.hpi.tfm.fact_merging.metrics.{MultipleEventWeightScore, MutualInformationScore}
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.{RuzickaSimilarityWildcardIgnore, TransitionMatchScore}
import de.hpi.tfm.util.CSVUtil

import java.io.{File, PrintWriter}
import java.time.LocalDate

class EdgeAnalyser(edges: collection.Seq[WikipediaInfoboxValueHistoryMatch],trainGraphConfig:GraphConfig,TIMESTAMP_RESOLUTION_IN_DAYS:Long) extends StrictLogging{

  def toCSVLine(e: WikipediaInfoboxValueHistoryMatch) = {
    val line = WikipediaEdgeStatRow(e,TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig)
    line.toCSVLine
  }

  def toCsvFile(f:File): Unit = {
    val pr = new PrintWriter(f)
    pr.println(WikipediaEdgeStatRow(edges.head,TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig).getSchema.mkString(","))
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
