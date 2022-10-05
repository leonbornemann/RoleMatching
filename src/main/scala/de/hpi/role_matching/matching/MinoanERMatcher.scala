package de.hpi.role_matching.matching

import de.hpi.role_matching.data.{LabelledRoleMatchCandidate, Roleset}
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate

class MinoanERMatcher(resultPR: PrintWriter, trainTimeEnd: LocalDate, rs:Roleset,inputDataPath:File) {

  val stringToLineageMap = rs
    .getStringToLineageMap
    .map(t => (t._1,t._2.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)))
  val stringToTokenMap = stringToLineageMap
    .map(t => (t._1,t._2.nonWildcardValueSetBefore(trainTimeEnd)))
  val tokenToFrequencyMap = stringToTokenMap
    .toIndexedSeq
    .flatMap(s => s._2.toIndexedSeq)
    .groupBy(identity)
    .map(t => (t._1,t._2.size))

  val thresholds = (1 until 100)
    .map(i => i / 100.0)

  def tokenFrequency(t: Any): Double = {
    tokenToFrequencyMap(t)
  }

  def log2(d: Double) = {
    Math.log(d) / Math.log(2)
  }

  def valueSimilarity(id1: String, id2: String) = {
    val tokens1 = stringToTokenMap(id1)
    val tokens2 = stringToTokenMap(id2)
    tokens1
      .intersect(tokens2)
      .map(t => 1.0 / log2(scala.math.pow(tokenFrequency(t),2))+1 )
      .sum
  }

  def valueMatchingRule(l: LabelledRoleMatchCandidate) = {
    val valueSim = valueSimilarity(l.id1,l.id2)
    valueSim
  }

  def neighbourMatchingRule(l: LabelledRoleMatchCandidate) = {
    //TODO
    ???
  }

  def appendPrecisionRecall() = {
    val examples = LabelledRoleMatchCandidate.fromJsonObjectPerLineFile(inputDataPath.getAbsolutePath)
    val withScores = examples
      .map(l => (l,valueMatchingRule(l),neighbourMatchingRule(l)))
    ???
  }
}
