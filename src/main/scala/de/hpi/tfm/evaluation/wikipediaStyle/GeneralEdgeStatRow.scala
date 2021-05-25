package de.hpi.tfm.evaluation.wikipediaStyle

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, ValueTransition}
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScore
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.{RuzickaSimilarity, TransitionHistogramMode, TransitionMatchScore}
import de.hpi.tfm.io.IOService
import de.hpi.tfm.util.CSVUtil

import java.time.LocalDate

case class GeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS:Int,
                              trainGraphConfig:GraphConfig,
                              edgeString1: String,
                              edgeString2: String,
                              v1: TemporalFieldTrait[Any],
                              v2: TemporalFieldTrait[Any],
                              nonInformativeValues:Set[Any],
                              transitionHistogramForTFIDF:Map[ValueTransition[Any],Int],
                              lineageCount:Int) {

  val histogramModes = Seq(TransitionHistogramMode.NORMAL,TransitionHistogramMode.IGNORE_NON_CHANGE,TransitionHistogramMode.COUNT_NON_CHANGE_ONLY_ONCE)
  val metricsTrain = /*histogramModes.flatMap(m => IndexedSeq(new RuzickaSimilarity[Any](TIMESTAMP_RESOLUTION_IN_DAYS,m),
    new TransitionMatchScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,m)) )++*/
    Seq(
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,false,None,None,None),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,false,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(false)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,false,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(true)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,true,None,None,None),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,true,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(false)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,true,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(true)))

//  val metricsFull = histogramModes.flatMap(m => IndexedSeq(new RuzickaSimilarity(TIMESTAMP_RESOLUTION_IN_DAYS,m),
//    new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,m))) ++ Seq(new MultipleEventWeightScore(TIMESTAMP_RESOLUTION_IN_DAYS,IOService.STANDARD_TIME_FRAME_END))

  val remainsValid = v1.tryMergeWithConsistent(v2).isDefined
  val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(v1).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(v2).isDefined
  val v1Train = v1.asInstanceOf[FactLineage].projectToTimeRange(trainGraphConfig.timeRangeStart,trainGraphConfig.timeRangeEnd)
  val v2Train = v2.asInstanceOf[FactLineage].projectToTimeRange(trainGraphConfig.timeRangeStart,trainGraphConfig.timeRangeEnd)
  val isNumeric = v1Train.isNumeric || v2Train.isNumeric
  def trainMetrics = metricsTrain.map(m => m.compute(v1Train,v2Train))
  //val computedMetricsFull = metricsFull.map(m => m.compute(v1,v2))

  def getSchema = {
    Seq("Vertex1ID,Vertex2ID") ++ Seq("remainsValid","hasChangeAfterTrainPeriod,isNumeric") ++ metricsTrain.map(_.name + "_TrainPeriod") //++ metricsFull.map(_.name + "_FullPeriod")
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
    (Seq(edgeString1,edgeString2) ++ Seq(remainsValid,isInteresting,isNumeric) ++ trainMetrics /*++ computedMetricsFull*/).map(CSVUtil.toCleanString(_)).mkString(",")
  }

}
