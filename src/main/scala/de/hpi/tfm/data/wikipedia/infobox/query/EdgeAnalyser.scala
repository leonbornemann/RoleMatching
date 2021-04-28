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

  val metrics = IndexedSeq(new RuzickaSimilarityWildcardIgnore(TIMESTAMP_RESOLUTION_IN_DAYS),new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,false),new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,true),new MultipleEventWeightScore)

  def getSchema = {
    val v1 = Seq("template1","pageID1","key1","property1")
    val v2 = Seq("template2","pageID2","key2","property2")
    v1 ++ v2 ++ Seq("remainsValid","hasChangeAfterTrainPeriod") ++ metrics.map(_.name)
  }

  def toCSVLine(e: WikipediaInfoboxValueHistoryMatch) = {
    val (v1,v2) = (e.a,e.b)
    val v1String = Seq(v1.template.getOrElse(""),v1.pageID.toString(),v1.key.toString,v1.p)
    val v2String = Seq(v2.template.getOrElse(""),v2.pageID.toString(),v2.key.toString,v2.p)
    val remainsValid = e.a.lineage.toFactLineage.tryMergeWithConsistent(e.b.lineage.toFactLineage).isDefined
    val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(e.a.lineage.toFactLineage).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(e.b.lineage.toFactLineage).isDefined
    val computedMetrics = metrics.map(m => m.compute(e.a.lineage.toFactLineage,e.b.lineage.toFactLineage))
    (v1String ++ v2String ++ Seq(remainsValid,isInteresting) ++ computedMetrics).map(CSVUtil.toCleanString(_)).mkString(",")
  }

  //Dirty: copied from HoldoutTimeEvaluator
  def getPointInTimeOfRealChangeAfterTrainPeriod(lineage: FactLineage) = {
    val prevNonWcValue = lineage.lineage.filter(t => !lineage.isWildcard(t._2) && !t._1.isAfter(trainGraphConfig.timeRangeEnd)).lastOption
    if(prevNonWcValue.isEmpty)
      None
    else {
      val it = lineage.lineage.iteratorFrom(trainGraphConfig.timeRangeEnd)
      var pointInTime:Option[LocalDate] = None
      while(it.hasNext && !pointInTime.isDefined){
        val (curTIme,curValue) = it.next()
        if(!lineage.isWildcard(curValue) && curValue!=prevNonWcValue.get._2){
          pointInTime = Some(curTIme)
        }
      }
      pointInTime
    }
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

}
