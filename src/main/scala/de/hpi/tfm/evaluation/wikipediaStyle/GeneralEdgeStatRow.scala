package de.hpi.tfm.evaluation.wikipediaStyle

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScore
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.{RuzickaSimilarityWildcardIgnore, TransitionMatchScore}
import de.hpi.tfm.util.CSVUtil

import java.time.LocalDate

case class GeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS:Int,trainGraphConfig:GraphConfig,edgeString1: String, edgeString2: String, v1: TemporalFieldTrait[Any], v2: TemporalFieldTrait[Any]) {

  val metrics = IndexedSeq(new RuzickaSimilarityWildcardIgnore(TIMESTAMP_RESOLUTION_IN_DAYS),new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,false),new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,true),new MultipleEventWeightScore)

  val remainsValid = v1.tryMergeWithConsistent(v2).isDefined
  val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(v1).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(v2).isDefined
  val computedMetrics = metrics.map(m => m.compute(v1,v2))

  def getSchema = {
    Seq("Vertex1ID,Vertex2ID") ++ Seq("remainsValid","hasChangeAfterTrainPeriod") ++ metrics.map(_.name)
  }

  //Dirty: copied from HoldoutTimeEvaluator
  def getPointInTimeOfRealChangeAfterTrainPeriod(lineage: TemporalFieldTrait[Any]) = {
    val prevNonWcValue = lineage.getValueLineage.filter(t => !lineage.isWildcard(t._2) && !t._1.isAfter(trainGraphConfig.timeRangeEnd)).lastOption
    if(prevNonWcValue.isEmpty)
      None
    else {
      val it = lineage.getValueLineage.iteratorFrom(trainGraphConfig.timeRangeEnd)
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
    if(!remainsValid && !isInteresting) {
      val interesting = this.v1.getValueLineage.keys.toSet.intersect(this.v2.getValueLineage.keys.toSet)
        .map(k => (this.v1.getValueLineage(k),this.v2.getValueLineage(k)))
      println()
    }
    (Seq(edgeString1 ++ edgeString2) ++ Seq(remainsValid,isInteresting) ++ computedMetrics).map(CSVUtil.toCleanString(_)).mkString(",")
  }

}
