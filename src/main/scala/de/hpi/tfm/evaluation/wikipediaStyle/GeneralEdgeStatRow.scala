package de.hpi.tfm.evaluation.wikipediaStyle

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{CommonPointOfInterestIterator, FactLineage, ValueTransition}
import de.hpi.tfm.evaluation.data.GeneralEdge
import de.hpi.tfm.fact_merging.metrics.{MultipleEventWeightScore, TFIDFWeightingVariant}
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
                              lineageCount:Int) extends StatComputer{

  val histogramModes = Seq(TransitionHistogramMode.NORMAL,TransitionHistogramMode.IGNORE_NON_CHANGE,TransitionHistogramMode.COUNT_NON_CHANGE_ONLY_ONCE)
  val metricsTrain = /*histogramModes.flatMap(m => IndexedSeq(new RuzickaSimilarity[Any](TIMESTAMP_RESOLUTION_IN_DAYS,m),
    new TransitionMatchScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,m)) )++*/
    Seq(
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,false,None,None,None),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,false,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(TFIDFWeightingVariant.LIN)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,false,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(TFIDFWeightingVariant.EXP)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,false,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(TFIDFWeightingVariant.DVD)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,true,None,None,None),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,true,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(TFIDFWeightingVariant.LIN)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,true,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(TFIDFWeightingVariant.EXP)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,trainGraphConfig.timeRangeEnd,nonInformativeValues,true,Some(transitionHistogramForTFIDF),Some(lineageCount),Some(TFIDFWeightingVariant.DVD)))

//  val metricsFull = histogramModes.flatMap(m => IndexedSeq(new RuzickaSimilarity(TIMESTAMP_RESOLUTION_IN_DAYS,m),
//    new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,m))) ++ Seq(new MultipleEventWeightScore(TIMESTAMP_RESOLUTION_IN_DAYS,IOService.STANDARD_TIME_FRAME_END))

  val remainsValidStrict = v1.tryMergeWithConsistent(v2,RemainsValidVariant.STRICT).isDefined
  val remainsValidContainment = v1.tryMergeWithConsistent(v2,RemainsValidVariant.CONTAINMENT).isDefined
  val remainsValid_0_9_PercentageOfTime = v1.isConsistentWith(v2,0.9)
  val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(v1,trainGraphConfig.timeRangeEnd).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(v2,trainGraphConfig.timeRangeEnd).isDefined

  val interestingnessEvidence = getEvidenceInTestPhase(v1,v2,trainGraphConfig.timeRangeEnd)
  if(!isInteresting && interestingnessEvidence>0) {
    println()
    GeneralEdge(v1.asInstanceOf[FactLineage].toIdentifiedFactLineage("#1"),v2.asInstanceOf[FactLineage].toIdentifiedFactLineage("#2")).printTabularEventLineageString
    val evidence = getEvidenceInTestPhase(v1,v2,trainGraphConfig.timeRangeEnd)
    println(evidence)
  }
  val v1Train = v1.asInstanceOf[FactLineage].projectToTimeRange(trainGraphConfig.timeRangeStart,trainGraphConfig.timeRangeEnd)
  val v2Train = v2.asInstanceOf[FactLineage].projectToTimeRange(trainGraphConfig.timeRangeStart,trainGraphConfig.timeRangeEnd)
  val isNumeric = v1Train.isNumeric || v2Train.isNumeric
  def trainMetrics = metricsTrain.map(m => m.compute(v1Train,v2Train))
  //val computedMetricsFull = metricsFull.map(m => m.compute(v1,v2))

  def getSchema = {
    Seq("Vertex1ID,Vertex2ID") ++ Seq("remainsValid","remainsValidContainment","remainsValid_0_9_PercentageOfTime","hasChangeAfterTrainPeriod","interestingnessEvidence","isNumeric") ++ metricsTrain.map(_.name + "_TrainPeriod") //++ metricsFull.map(_.name + "_FullPeriod")
  }

  def toCSVLine = {
    (Seq(edgeString1,edgeString2) ++ Seq(remainsValidStrict,remainsValidContainment,remainsValid_0_9_PercentageOfTime,isInteresting,interestingnessEvidence,isNumeric) ++ trainMetrics /*++ computedMetricsFull*/).map(CSVUtil.toCleanString(_)).mkString(",")
  }

}
