package de.hpi.tfm.data.wikipedia.infobox.query

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.wikipedia.infobox.query.QueryAnalysis.edges
import de.hpi.tfm.fact_merging.metrics.{MultipleEventWeightScore, MutualInformationScore}
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.RuzickaSimilarityWildcardIgnore
import de.hpi.tfm.util.CSVUtil

import java.io.{File, PrintWriter}

class EdgeAnalyser(edges: collection.Seq[WikipediaInfoboxValueHistoryMatch]) extends StrictLogging{

  val metrics = IndexedSeq(new RuzickaSimilarityWildcardIgnore,new MultipleEventWeightScore,new MutualInformationScore)

  def getSchema = {
    val v1 = Seq("template1","pageID1","key1","property1")
    val v2 = Seq("template2","pageID2","key2","property2")
    v1 ++ v2 ++ metrics.map(_.name)
  }

  def toCSVLine(e: WikipediaInfoboxValueHistoryMatch) = {
    val (v1,v2) = (e.a,e.b)
    val v1String = Seq(v1.template.getOrElse(""),v1.pageID.toString(),v1.key.toString,v1.p)
    val v2String = Seq(v2.template.getOrElse(""),v2.pageID.toString(),v2.key.toString,v2.p)
    val computedMetrics = metrics.map(m => m.compute(e.a.lineage.toFactLineage,e.b.lineage.toFactLineage))
    (v1String ++ v2String ++ computedMetrics).map(CSVUtil.toCleanString(_)).mkString(",")
  }

  def toCsvFile(f:File): Unit = {
    val pr = new PrintWriter(f)
    pr.println(getSchema.mkString(","))
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

  edges.map(e => EdgeAnalysis(e))

}
