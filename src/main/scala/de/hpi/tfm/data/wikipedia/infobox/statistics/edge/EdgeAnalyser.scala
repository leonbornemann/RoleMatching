package de.hpi.tfm.data.wikipedia.infobox.statistics.edge

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.wikipedia.infobox.query.WikipediaInfoboxValueHistoryMatch
import de.hpi.tfm.data.wikipedia.infobox.statistics.edge

import java.io.{File, PrintWriter}

class EdgeAnalyser(edges: collection.Seq[WikipediaInfoboxValueHistoryMatch],trainGraphConfig:GraphConfig,TIMESTAMP_RESOLUTION_IN_DAYS:Int) extends StrictLogging{

  def toCSVLine(e: WikipediaInfoboxValueHistoryMatch) = {
    val line = WikipediaEdgeStatRow(e,TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig).toGeneralStatRow
    line.toCSVLine
  }

  def toCsvFile(f:File): Unit = {
    val pr = new PrintWriter(f)
    pr.println(edge.WikipediaEdgeStatRow(edges.head,TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig).toGeneralStatRow.getSchema.mkString(","))
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
