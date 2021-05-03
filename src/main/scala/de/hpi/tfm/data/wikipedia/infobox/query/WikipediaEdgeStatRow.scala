package de.hpi.tfm.data.wikipedia.infobox.query

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScore
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.{RuzickaSimilarityWildcardIgnore, TransitionMatchScore}
import de.hpi.tfm.util.CSVUtil

import java.time.LocalDate

case class WikipediaEdgeStatRow(e: WikipediaInfoboxValueHistoryMatch,TIMESTAMP_RESOLUTION_IN_DAYS:Long,trainGraphConfig:GraphConfig) {
  val (v1,v2) = (e.a,e.b)
  val metrics = IndexedSeq(new RuzickaSimilarityWildcardIgnore(TIMESTAMP_RESOLUTION_IN_DAYS),new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,false),new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,true),new MultipleEventWeightScore)

  val v1String = Seq(v1.template.getOrElse(""),v1.pageID.toString(),v1.key.toString,v1.p)
  val v2String = Seq(v2.template.getOrElse(""),v2.pageID.toString(),v2.key.toString,v2.p)
  val remainsValid = e.a.lineage.toFactLineage.tryMergeWithConsistent(e.b.lineage.toFactLineage).isDefined
  val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(e.a.lineage.toFactLineage).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(e.b.lineage.toFactLineage).isDefined
  val computedMetrics = metrics.map(m => m.compute(e.a.lineage.toFactLineage,e.b.lineage.toFactLineage))

  def getSchema = {
    val v1 = Seq("template1","pageID1","key1","property1")
    val v2 = Seq("template2","pageID2","key2","property2")
    v1 ++ v2 ++ Seq("remainsValid","hasChangeAfterTrainPeriod") ++ metrics.map(_.name)
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

  def toCSVLine = {
    if(!remainsValid && !isInteresting)
      println()
    (v1String ++ v2String ++ Seq(remainsValid,isInteresting) ++ computedMetrics).map(CSVUtil.toCleanString(_)).mkString(",")
  }


}
object WikipediaEdgeStatRow{

}
