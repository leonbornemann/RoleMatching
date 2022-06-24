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
                          trainTimeEnd:LocalDate) {
  def appendStatRow(resultPR: PrintWriter) = {
    resultPR.println(s"$dataset,$id1,$id2,$isInStrictBlocking,$isInStrictBlockingNoDecay,$isInValueSetBlocking,$isInSequenceBlocking,$isInExactSequenceBlocking,$label," +
      s"$decayedCompatibilityPercentage," +
      s"${rl1ProjectedNoDecay.getCompatibilityTimePercentage(rl2ProjectedNoDecay,trainTimeEnd)}," +
      s"$exactSequenceMatchPercentage,$hasTransitionOverlapNoDecay,$hasTransitionOverlapDecay,$hasValueSetOverlap,$isInSVABlockingNoDecay,$isInSVABlockingDecay,"+
      s"$vaCOunt,$daCount")
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

}

object RoleMatchStatistics{

  def appendSchema(resultPr:PrintWriter) = {
    resultPr.println("dataset,id1,id2,isInStrictBlockingDecay,isInStrictBlockingNoDecay,isInValueSetBlocking,isInSequenceBlocking," +
      "isInExactMatchBlocking,isSemanticRoleMatch,compatibilityPercentageDecay,compatibilityPercentageNoDecay,exactSequenceMatchPercentage," +
      "hasTransitionOverlapNoDecay,hasTransitionOverlapDecay,hasValueSetOverlap,isInSVABlockingNoDecay,isInSVABlockingDecay,VACount,DVACount")
  }

}
