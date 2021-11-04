package de.hpi.role_matching.evaluation.tuning

import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple
import de.hpi.role_matching.cbrm.data.{RoleLineage, ValueTransition}
import de.hpi.role_matching.cbrm.evidence_based_weighting.EvidenceBasedWeighingScore
import de.hpi.role_matching.evaluation.matching.StatComputer
import de.hpi.util.CSVUtil

case class EdgeStatRow(TIMESTAMP_RESOLUTION_IN_DAYS: Int,
                       trainGraphConfig: GraphConfig,
                       edgeString1: String,
                       edgeString2: String,
                       v1: RoleLineage,
                       v2: RoleLineage,
                       nonInformativeValues: Set[Any],
                       transitionHistogramForTFIDF: Map[ValueTransition, Int],
                       lineageCount: Int) extends StatComputer {

  val metricsTrain = /*histogramModes.flatMap(m => IndexedSeq(new RuzickaSimilarity(TIMESTAMP_RESOLUTION_IN_DAYS,m),
    new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,m)) )++*/
    Seq(
      new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, false, None, None),
      new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, false, Some(transitionHistogramForTFIDF), Some(lineageCount)),
      new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, false, Some(transitionHistogramForTFIDF), Some(lineageCount)),
      new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, false, Some(transitionHistogramForTFIDF), Some(lineageCount)),
      new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, true, None, None),
      new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, true, Some(transitionHistogramForTFIDF), Some(lineageCount)),
      new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, true, Some(transitionHistogramForTFIDF), Some(lineageCount)),
      new EvidenceBasedWeighingScore(TIMESTAMP_RESOLUTION_IN_DAYS, trainGraphConfig.timeRangeEnd, nonInformativeValues, true, Some(transitionHistogramForTFIDF), Some(lineageCount)))

  //  val metricsFull = histogramModes.flatMap(m => IndexedSeq(new RuzickaSimilarity(TIMESTAMP_RESOLUTION_IN_DAYS,m),
  //    new TransitionMatchScore(TIMESTAMP_RESOLUTION_IN_DAYS,m))) ++ Seq(new MultipleEventWeightScore(TIMESTAMP_RESOLUTION_IN_DAYS,IOService.STANDARD_TIME_FRAME_END))

  val remainsValidStrict = v1.tryMergeWithConsistent(v2, RemainsValidVariant.STRICT).isDefined
  val remainsValidContainment = v1.tryMergeWithConsistent(v2, RemainsValidVariant.CONTAINMENT).isDefined
  val remainsValid_0_9_PercentageOfTime = v1.isConsistentWith(v2, 0.9)
  val isInteresting = getPointInTimeOfRealChangeAfterTrainPeriod(v1, trainGraphConfig.timeRangeEnd).isDefined || getPointInTimeOfRealChangeAfterTrainPeriod(v2, trainGraphConfig.timeRangeEnd).isDefined

  val interestingnessEvidence = getEvidenceInTestPhase(v1, v2, trainGraphConfig.timeRangeEnd)
  if (!isInteresting && interestingnessEvidence > 0) {
    println()
    simple.SimpleCompatbilityGraphEdge(v1.asInstanceOf[RoleLineage].toIdentifiedRoleLineage("#1"), v2.asInstanceOf[RoleLineage].toIdentifiedRoleLineage("#2")).printTabularEventLineageString
    val evidence = getEvidenceInTestPhase(v1, v2, trainGraphConfig.timeRangeEnd)
    println(evidence)
  }
  val v1Train = v1.asInstanceOf[RoleLineage].projectToTimeRange(trainGraphConfig.timeRangeStart, trainGraphConfig.timeRangeEnd)
  val v2Train = v2.asInstanceOf[RoleLineage].projectToTimeRange(trainGraphConfig.timeRangeStart, trainGraphConfig.timeRangeEnd)
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
