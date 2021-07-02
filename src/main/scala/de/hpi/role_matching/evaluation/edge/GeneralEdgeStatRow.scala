package de.hpi.role_matching.evaluation.edge

import de.hpi.socrata.tfmp_input.table.TemporalFieldTrait
import de.hpi.socrata.tfmp_input.table.nonSketch.{FactLineage, ValueTransition}
import de.hpi.role_matching.compatibility.GraphConfig
import de.hpi.role_matching.compatibility.graph.representation.simple
import de.hpi.role_matching.evaluation.StatComputer
import de.hpi.role_matching.scoring.{MultipleEventWeightScore, TFIDFWeightingVariant}
import de.hpi.util.CSVUtil

case class GeneralEdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS: Int,
                              trainGraphConfig: GraphConfig,
                              edgeString1: String,
                              edgeString2: String,
                              v1: TemporalFieldTrait[Any],
                              v2: TemporalFieldTrait[Any],
                              nonInformativeValues: Set[Any],
                              transitionHistogramForTFIDF: Map[ValueTransition[Any], Int],
                              lineageCount: Int) extends StatComputer {

  val metricsTrain = /*histogramModes.flatMap(m => IndexedSeq(new RuzickaSimilarity[Any](TIMESTAMP_RESOLUTION_IN_DAYS,m),
    new TransitionMatchScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS,m)) )++*/
    Seq(
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, false, None, None, None),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, false, Some(transitionHistogramForTFIDF), Some(lineageCount), Some(TFIDFWeightingVariant.LIN)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, false, Some(transitionHistogramForTFIDF), Some(lineageCount), Some(TFIDFWeightingVariant.EXP)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, false, Some(transitionHistogramForTFIDF), Some(lineageCount), Some(TFIDFWeightingVariant.DVD)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, true, None, None, None),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, true, Some(transitionHistogramForTFIDF), Some(lineageCount), Some(TFIDFWeightingVariant.LIN)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, true, Some(transitionHistogramForTFIDF), Some(lineageCount), Some(TFIDFWeightingVariant.EXP)),
      new MultipleEventWeightScore[Any](TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, true, Some(transitionHistogramForTFIDF), Some(lineageCount), Some(TFIDFWeightingVariant.DVD)))

  //  val metricsFull = histogramModes.flatMap(m => IndexedSeq(new RuzickaSimilarity(TIMESTAMP_RESOLUTION_IN_DAYS,m),
  //    new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,m))) ++ Seq(new MultipleEventWeightScore(TIMESTAMP_RESOLUTION_IN_DAYS,IOService.STANDARD_TIME_FRAME_END))

  val remainsValidStrict = v1.tryMergeWithConsistent(v2, RemainsValidVariant.STRICT).isDefined
  val remainsValidContainment = v1.tryMergeWithConsistent(v2, RemainsValidVariant.CONTAINMENT).isDefined
  val remainsValid_0_9_PercentageOfTime = v1.isConsistentWith(v2, 0.9)
  val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(v1, trainGraphConfig.timeRangeEnd).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(v2, trainGraphConfig.timeRangeEnd).isDefined

  val interestingnessEvidence = getEvidenceInTestPhase(v1, v2, trainGraphConfig.timeRangeEnd)
  if (!isInteresting && interestingnessEvidence > 0) {
    println()
    simple.GeneralEdge(v1.asInstanceOf[FactLineage].toIdentifiedFactLineage("#1"), v2.asInstanceOf[FactLineage].toIdentifiedFactLineage("#2")).printTabularEventLineageString
    val evidence = getEvidenceInTestPhase(v1, v2, trainGraphConfig.timeRangeEnd)
    println(evidence)
  }
  val v1Train = v1.asInstanceOf[FactLineage].projectToTimeRange(trainGraphConfig.timeRangeStart, trainGraphConfig.timeRangeEnd)
  val v2Train = v2.asInstanceOf[FactLineage].projectToTimeRange(trainGraphConfig.timeRangeStart, trainGraphConfig.timeRangeEnd)
  val isNumeric = v1Train.isNumeric || v2Train.isNumeric

  def trainMetrics = metricsTrain.map(m => m.compute(v1Train, v2Train))
  //val computedMetricsFull = metricsFull.map(m => m.compute(v1,v2))

  def getSchema = {
    Seq("Vertex1ID,Vertex2ID") ++ Seq("remainsValid", "remainsValidContainment", "remainsValid_0_9_PercentageOfTime", "hasChangeAfterTrainPeriod", "interestingnessEvidence", "isNumeric") ++ metricsTrain.map(_.name + "_TrainPeriod") //++ metricsFull.map(_.name + "_FullPeriod")
  }

  def toCSVLine = {
    (Seq(edgeString1, edgeString2) ++ Seq(remainsValidStrict, remainsValidContainment, remainsValid_0_9_PercentageOfTime, isInteresting, interestingnessEvidence, isNumeric) ++ trainMetrics /*++ computedMetricsFull*/).map(CSVUtil.toCleanString(_)).mkString(",")
  }

}
