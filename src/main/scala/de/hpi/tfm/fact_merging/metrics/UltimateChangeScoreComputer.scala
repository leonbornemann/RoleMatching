package de.hpi.tfm.fact_merging.metrics

import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{CommonPointOfInterestIterator, ValueTransition}

class UltimateChangeScoreComputer[A](a:TemporalFieldTrait[A],b:TemporalFieldTrait[A]) {

  val WILDCARD_TO_KNOWN_TRANSITION_WEIGHT = -0.05
  val WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT = -0.1
  val BOTH_WILDCARD_WEIGHT = -0.01
  val SYNCHRONOUS_NON_WILDCARD_TRANSITION = 1.0
  val transitionSetA = a.valueTransitions(true)
  val transitionSetB = b.valueTransitions(true)
  var totalScore = 0.0

  computeScore()


  def computeScore() = {
    val commonPointOfInterestIterator = new CommonPointOfInterestIterator[A](a,b)
    commonPointOfInterestIterator.foreach(cp => {
      //handle previous transitions:
      val countPrev = cp.pointInTime.toEpochDay - cp.prevPointInTime.toEpochDay - 1
      val prevValueA = cp.prevValueA
      val prevValueB = cp.prevValueB
      handleSameValueTransitions(prevValueA,prevValueB,countPrev)
      //handle transition:
      val noWildcardInTransition = Set(prevValueA,prevValueB,cp.curValueA,cp.curValueB).forall(v => !a.isWildcard(v))
      if(noWildcardInTransition){
        totalScore +=1*SYNCHRONOUS_NON_WILDCARD_TRANSITION
      } else {
        val aChanged = cp.curValueA!=cp.prevValueA && !a.isWildcard(cp.curValueA) && !a.isWildcard(cp.prevValueA)
        val bChanged = cp.curValueB!=cp.prevValueB && !a.isWildcard(cp.curValueB) && !a.isWildcard(cp.prevValueB)
        if (aChanged) {
          if(transitionSetB.contains(ValueTransition(cp.prevValueA,cp.curValueA))){
            totalScore+=countPrev*WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
          } else {
            totalScore+=countPrev*WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
          }
        } else if(bChanged) {
          if(transitionSetA.contains(ValueTransition(cp.prevValueB,cp.curValueB))){
            totalScore+=countPrev*WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
          } else {
            totalScore+=countPrev*WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
          }
        } else {
          totalScore += countPrev * WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
        }
      }
    })
  }

  def handleSameValueTransitions(prevValueA: A, prevValueB: A, countPrev: Long) = {
    if(a.isWildcard(prevValueA) && a.isWildcard(prevValueB)){
      totalScore += countPrev*BOTH_WILDCARD_WEIGHT
    } else if(a.isWildcard(prevValueA)){
      if(transitionSetA.contains(ValueTransition(prevValueB,prevValueB))){
        totalScore+=countPrev*WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
      } else {
        totalScore+=countPrev*WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
      }
    } else if(a.isWildcard(prevValueB)){
      if(transitionSetB.contains(ValueTransition(prevValueA,prevValueA))){
        totalScore+=countPrev*WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
      } else {
        totalScore+=countPrev*WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
      }
    } else {
      //do nothing - score remains 0
    }
  }

  def score():Double = {
    totalScore
  }

}
