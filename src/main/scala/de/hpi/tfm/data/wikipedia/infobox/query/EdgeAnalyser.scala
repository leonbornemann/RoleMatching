package de.hpi.tfm.data.wikipedia.infobox.query

import de.hpi.tfm.data.wikipedia.infobox.query.QueryAnalysis.edges
import de.hpi.tfm.fact_merging.metrics.{MultipleEventWeightScore, MutualInformationScore}
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.RuzickaSimilarityWildcardIgnore
import de.hpi.tfm.util.CSVUtil

import java.io.{File, PrintWriter}

class EdgeAnalyser(edges: collection.Seq[WikipediaInfoboxValueHistoryMatch]) {

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
    (v1String ++ v2String ++ computedMetrics).map(CSVUtil.toCleanString(_))
  }

  def toCsvFile(f:File): Unit = {
    val pr = new PrintWriter(f)
    pr.println(getSchema.mkString(","))
    edges.foreach(e => {
      pr.println(toCSVLine(e))
    })
    pr.close()
  }

  edges.map(e => EdgeAnalysis(e))

}
