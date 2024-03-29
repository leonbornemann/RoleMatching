package de.hpi.role_matching.evaluation.blocking.ground_truth

import de.hpi.role_matching.data.{RoleLineage, RoleLineageWithID, RoleMatchCandidate}
import de.hpi.util.GLOBAL_CONFIG

import java.io.PrintWriter
import java.time.LocalDate

class RoleMatchStatistics(dataset:String,
                          edgeNoDecay: RoleMatchCandidate,
                          label: Boolean,
                          trainTimeEnd:LocalDate
                          ) {

  val decayThresholds = (0 to 200 )
    .map(i => i / 200.0)
    .reverse

  def appendStatRow(resultPR: PrintWriter) = {
    resultPR.println(s"$dataset,$id1,$id2,$isInStrictBlockingNoDecay,$isInValueSetBlocking,$isInSequenceBlocking,$isInExactSequenceBlocking,$label," +
      s"${rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)}," +
      s"$exactSequenceMatchPercentage,$hasTransitionOverlapNoDecay,$isInSVABlockingNoDecay,"+
      s"$vaCOunt,$daCount,$isInTSMBlockingNoWildcard,$isInTSMBlockingWithWildcard,$strictlyCompatiblePercentage,$decayScore,$hasNonOverlap")
  }

  def getDecayedEdgeFromUndecayedEdge(edgeNoDecay: RoleMatchCandidate, beta:Double):RoleMatchCandidate = {
    val rl1WithDecay = edgeNoDecay.v1.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    val rl2WithDecay = edgeNoDecay.v2.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd)
    RoleMatchCandidate(RoleLineageWithID(edgeNoDecay.v1.id,rl1WithDecay.toSerializationHelper),RoleLineageWithID(edgeNoDecay.v2.id,rl2WithDecay.toSerializationHelper))
  }

  val id1 = edgeNoDecay.v1.csvSafeID
  val id2 = edgeNoDecay.v2.csvSafeID
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
  val isInStrictBlockingNoDecay = statRowNoDecay.remainsValidFullTimeSpan
  val nonDecayCompatibilityPercentage = rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)
  val exactSequenceMatchPercentage = rl1ProjectedNoDecay.exactMatchesWithoutWildcardPercentage(rl2ProjectedNoDecay,trainTimeEnd)
  val vaCOunt = rl1ProjectedNoDecay.exactMatchWithoutWildcardCount(rl2ProjectedNoDecay,trainTimeEnd,false)
  val daCount = rl1ProjectedNoDecay.exactDistinctMatchWithoutWildcardCount(rl2ProjectedNoDecay,trainTimeEnd)
  val isInTSMBlockingNoWildcard = rl1ProjectedNoDecay.getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd)) == rl2ProjectedNoDecay.getValueTransitionSet(true,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd))
  val isInTSMBlockingWithWildcard = rl1ProjectedNoDecay.getValueTransitionSet(false,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd)) == rl2ProjectedNoDecay.getValueTransitionSet(false,GLOBAL_CONFIG.granularityInDays,Some(trainTimeEnd))
  val strictlyCompatiblePercentage = rl1ProjectedNoDecay.getStrictCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)
  val decayScore = RoleMatchStatistics.getDecayScore(rl1ProjectedNoDecay,rl2ProjectedNoDecay,decayThresholds,trainTimeEnd)
  val hasNonOverlap = rl1ProjectedNoDecay.presenceTimeDoesNotExactlyMatch(rl2ProjectedNoDecay,trainTimeEnd)
}

object RoleMatchStatistics{

  def appendSchema(resultPr:PrintWriter) = {
    resultPr.println("dataset,id1,id2,isInStrictBlockingNoDecay,isInValueSetBlocking,isInSequenceBlocking," +
      "isInExactMatchBlocking,isSemanticRoleMatch,compatibilityPercentageNoDecay,exactSequenceMatchPercentage," +
      "hasTransitionOverlapNoDecay,isInSVABlockingNoDecay,VACount,DVACount,"+
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
