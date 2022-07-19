package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleLineageWithID}
import de.hpi.role_matching.evaluation.tuning.BasicStatRow

import java.io.PrintWriter
import java.time.LocalDate

class RoleMatchStatistics(dataset:String,
                          edgeNoDecay: SimpleCompatbilityGraphEdge,
                          label: Boolean,
                          DECAY_THRESHOLD: Double,
                          DECAY_THRESHOLD_SCB: Double,
                          trainTimeEnd:LocalDate) {

  val decayThresholds = (0 to 200 )
    .map(i => i / 200.0)
    .reverse

  def appendStatRow(resultPR: PrintWriter) = {
    resultPR.println(s"$dataset,$id1,$id2,$isInStrictBlocking,$isInStrictBlockingNoDecay,$isInValueSetBlocking,$isInSequenceBlocking,$isInExactSequenceBlocking,$label," +
      s"$decayedCompatibilityPercentage," +
      s"${rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)}," +
      s"$exactSequenceMatchPercentage,$hasTransitionOverlapNoDecay,$hasTransitionOverlapDecay,$hasValueSetOverlap,$isInSVABlockingNoDecay,$isInSVABlockingDecay,"+
      s"$vaCOunt,$daCount,$isInTSMBlockingNoWildcard,$isInTSMBlockingWithWildcard,$strictlyCompatiblePercentage,$decayScore,$hasNonOverlap")
  }

  def getDecayedEdgeFromUndecayedEdge(edgeNoDecay: SimpleCompatbilityGraphEdge,beta:Double):SimpleCompatbilityGraphEdge = {
    val rl1WithDecay = edgeNoDecay.v1.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    val rl2WithDecay = edgeNoDecay.v2.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    SimpleCompatbilityGraphEdge(RoleLineageWithID(edgeNoDecay.v1.id,rl1WithDecay.toSerializationHelper),RoleLineageWithID(edgeNoDecay.v2.id,rl2WithDecay.toSerializationHelper))
  }

  val edgeDecay = getDecayedEdgeFromUndecayedEdge(edgeNoDecay,DECAY_THRESHOLD)
  val id1 = edgeDecay.v1.csvSafeID
  val id2 = edgeDecay.v2.csvSafeID
  val rl1 = edgeDecay.v1.roleLineage.toRoleLineage
  val rl2 = edgeDecay.v2.roleLineage.toRoleLineage
  val rl1Projected = rl1.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
  val rl2Projected = rl2.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
  val hasTransitionOverlapDecay = rl1Projected.informativeValueTransitions.intersect(rl2Projected.informativeValueTransitions).size>=1
  val statRow = new BasicStatRow(rl1Projected,rl2Projected,trainTimeEnd)
  val hasValueSetOverlap = rl1Projected.nonWildcardValueSetBefore(trainTimeEnd.plusDays(1)).exists(v => rl2Projected.nonWildcardValueSetBefore(trainTimeEnd.plusDays(1)).contains(v))
  val isInSVABlockingDecay = GLOBAL_CONFIG.STANDARD_TIME_RANGE
    .filter(!_.isAfter(trainTimeEnd))
    .exists(t => rl1Projected.valueAt(t) == rl2Projected.valueAt(t) && !RoleLineage.isWildcard(rl1Projected.valueAt(t)))
  //no decay:
  val rl1NoDecay = edgeNoDecay.v1.roleLineage.toRoleLineage
  val rl2NoDecay = edgeNoDecay.v2.roleLineage.toRoleLineage
  val rl1ProjectedNoDecay = rl1NoDecay.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
  val rl2ProjectedNoDecay = rl2NoDecay.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
  val hasTransitionOverlapNoDecay = rl1ProjectedNoDecay.informativeValueTransitions.intersect(rl2ProjectedNoDecay.informativeValueTransitions).size>=1
  val statRowNoDecay = new BasicStatRow(rl1ProjectedNoDecay,rl2ProjectedNoDecay,trainTimeEnd)
  val isInSVABlockingNoDecay = GLOBAL_CONFIG.STANDARD_TIME_RANGE
    .filter(!_.isAfter(trainTimeEnd))
    .exists(t => rl1ProjectedNoDecay.valueAt(t) == rl2ProjectedNoDecay.valueAt(t) && !RoleLineage.isWildcard(rl1ProjectedNoDecay.valueAt(t)))
  //csv fields:
  val isInValueSetBlocking = rl1ProjectedNoDecay.nonWildcardValueSetBefore(trainTimeEnd) == rl2ProjectedNoDecay.nonWildcardValueSetBefore(trainTimeEnd)
  val isInSequenceBlocking = rl1ProjectedNoDecay.valueSequenceBefore(trainTimeEnd) == rl2ProjectedNoDecay.valueSequenceBefore(trainTimeEnd)
  val isInExactSequenceBlocking = rl1ProjectedNoDecay.exactlyMatchesWithoutDecay(rl2ProjectedNoDecay,trainTimeEnd)
  val isInStrictBlocking = statRow.remainsValidFullTimeSpan
  val isInStrictBlockingNoDecay = statRowNoDecay.remainsValidFullTimeSpan
  val decayedCompatibilityPercentage = rl1Projected.getCompatibilityTimePercentage(rl2Projected, trainTimeEnd)
  val nonDecayCompatibilityPercentage = rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)
  val exactSequenceMatchPercentage = rl1ProjectedNoDecay.exactMatchesWithoutWildcardPercentage(rl2ProjectedNoDecay,trainTimeEnd)
  val vaCOunt = rl1Projected.exactMatchWithoutWildcardCount(rl2ProjectedNoDecay,trainTimeEnd,false)
  val daCount = rl1Projected.exactDistinctMatchWithoutWildcardCount(rl2ProjectedNoDecay,trainTimeEnd)
  val isInTSMBlockingNoWildcard = rl1Projected.getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd)) == rl2Projected.getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd))
  val isInTSMBlockingWithWildcard = rl1Projected.getValueTransitionSet(false,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd)) == rl2Projected.getValueTransitionSet(false,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd))
  val strictlyCompatiblePercentage = rl1ProjectedNoDecay.getStrictCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)
  val edgeDecaySCB = getDecayedEdgeFromUndecayedEdge(edgeNoDecay,DECAY_THRESHOLD_SCB)
  val rl1ProjectedSCBDecay = edgeDecaySCB.v1.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
  val rl2ProjectedSCBDecay = edgeDecaySCB.v2.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)
  val strictlyCompatiblePercentageWithDecay = rl1ProjectedSCBDecay.getStrictCompatibilityTimePercentage(rl2ProjectedSCBDecay,trainTimeEnd)
  val decayScore = RoleMatchStatistics.getDecayScore(rl1ProjectedNoDecay,rl2ProjectedNoDecay,decayThresholds,trainTimeEnd)
  val hasNonOverlap = rl1ProjectedNoDecay.presenceTimeDoesNotExactlyMatch(rl2ProjectedNoDecay,trainTimeEnd)
}

object RoleMatchStatistics{

  def appendSchema(resultPr:PrintWriter) = {
    resultPr.println("dataset,id1,id2,isInStrictBlockingDecay,isInStrictBlockingNoDecay,isInValueSetBlocking,isInSequenceBlocking," +
      "isInExactMatchBlocking,isSemanticRoleMatch,compatibilityPercentageDecay,compatibilityPercentageNoDecay,exactSequenceMatchPercentage," +
      "hasTransitionOverlapNoDecay,hasTransitionOverlapDecay,hasValueSetOverlap,isInSVABlockingNoDecay,isInSVABlockingDecay,VACount,DVACount,"+
      "isInTSMBlockingNoWildcard,isInTSMBlockingWithWildcard,strictlyCompatiblePercentage,decayScore,hasNonOverlap")
  }

  //decay thresholds must be descending!
  def getDecayScore(rl1: RoleLineage, rl2: RoleLineage,decayThresholds:IndexedSeq[Double],trainTimeEnd:LocalDate) = {
    val it = decayThresholds.iterator
    var curThreshold = -2.0
    while(curThreshold == -2.0){
      if(it.hasNext){
        val cur = it.next()
        val rl1Decayed = rl1.applyDecay(cur,trainTimeEnd)
        val rl2Decayed = rl2.applyDecay(cur,trainTimeEnd)
        val statRow = new BasicStatRow(rl1Decayed,rl2Decayed,trainTimeEnd)
        if(statRow.remainsValidFullTimeSpan) {
          curThreshold=cur
        }
      } else {
        curThreshold = -1.0
      }
    }
    curThreshold
  }

}
