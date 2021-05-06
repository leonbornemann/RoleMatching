package de.hpi.tfm.fact_merging.metrics

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{CommonPointOfInterestIterator, ValueTransition}
import de.hpi.tfm.io.IOService

import java.time.LocalDate

class MultipleEventWeightScoreComputer[A](a:TemporalFieldTrait[A],
                                          b:TemporalFieldTrait[A],
                                          val TIMESTAMP_GRANULARITY_IN_DAYS:Int,
                                          timeEnd:LocalDate // this should be the end of train time!
                                         ) {

  val totalTransitionCount = (IOService.STANDARD_TIME_FRAME_START.toEpochDay until timeEnd.toEpochDay by TIMESTAMP_GRANULARITY_IN_DAYS).size-1
  val WILDCARD_TO_KNOWN_TRANSITION_WEIGHT = -0.1 / totalTransitionCount
  val WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT = -0.5 / totalTransitionCount
  val BOTH_WILDCARD_WEIGHT = 0
  val SYNCHRONOUS_NON_WILDCARD_CHANGE_TRANSITION = 0.5 / totalTransitionCount
  val SYNCHRONOUS_NON_WILDCARD_NON_CHANGE_TRANSITION = 0.1 / totalTransitionCount
  val transitionSetA = a.valueTransitions(true,false)
  val transitionSetB = b.valueTransitions(true,false)
  var totalScore = 0.5
  var totalScoreChanges =0

  computeScore()


  private def computeScore() = {
    if(!a.tryMergeWithConsistent(b).isDefined){
      totalScore = MultipleEventWeightScoreComputer.scoreForInconsistent
    } else {
      val commonPointOfInterestIterator = new CommonPointOfInterestIterator[A](a,b)
      commonPointOfInterestIterator.foreach(cp => {
        //handle previous transitions:
        val countPrevInDays = cp.pointInTime.toEpochDay - cp.prevPointInTime.toEpochDay - TIMESTAMP_GRANULARITY_IN_DAYS
        if(!(countPrevInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0))
          println()
        assert(countPrevInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0)
        val countPrev = countPrevInDays / TIMESTAMP_GRANULARITY_IN_DAYS
        val prevValueA = cp.prevValueA
        val prevValueB = cp.prevValueB
        handleSameValueTransitions(prevValueA,prevValueB,countPrev.toInt)
        //handle transition:
        val noWildcardInTransition = Set(prevValueA,prevValueB,cp.curValueA,cp.curValueB).forall(v => !a.isWildcard(v))
        if(noWildcardInTransition){
          totalScore +=1*SYNCHRONOUS_NON_WILDCARD_CHANGE_TRANSITION
          totalScoreChanges+=1
        } else {
          val aChanged = cp.curValueA!=cp.prevValueA && !a.isWildcard(cp.curValueA) && !a.isWildcard(cp.prevValueA)
          val bChanged = cp.curValueB!=cp.prevValueB && !a.isWildcard(cp.curValueB) && !a.isWildcard(cp.prevValueB)
          if (aChanged) {
            if(transitionSetB.contains(ValueTransition(cp.prevValueA,cp.curValueA))){
              totalScore+=1*WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
              totalScoreChanges+=1
            } else {
              totalScore+=1*WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
              totalScoreChanges+=1
            }
          } else if(bChanged) {
            if(transitionSetA.contains(ValueTransition(cp.prevValueB,cp.curValueB))){
              totalScore+=1*WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
              totalScoreChanges+=1
            } else {
              totalScore+=1*WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
              totalScoreChanges+=1
            }
          } else {
            totalScore += 1 * WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
            totalScoreChanges+=1
          }
        }
      })
      val lastKey = Seq(a.getValueLineage.lastKey,b.getValueLineage.lastKey).maxBy(_.toEpochDay)
      val lastValueA = a.getValueLineage.last._2
      val lastValueB = b.getValueLineage.last._2
      val countLastInDays = timeEnd.toEpochDay - lastKey.toEpochDay - TIMESTAMP_GRANULARITY_IN_DAYS
      if(!(countLastInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0))
        println()
      assert(countLastInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0)
      val countLast = countLastInDays / TIMESTAMP_GRANULARITY_IN_DAYS
      handleSameValueTransitions(lastValueA,lastValueB,countLast.toInt)
    }
  }

  def handleSameValueTransitions(prevValueA: A, prevValueB: A, countPrev: Int) = {
    if(a.isWildcard(prevValueA) && a.isWildcard(prevValueB)){
      totalScore += countPrev*BOTH_WILDCARD_WEIGHT
      totalScoreChanges+=countPrev
    } else if(a.isWildcard(prevValueA)){
      if(transitionSetA.contains(ValueTransition(prevValueB,prevValueB))){
        totalScore+=countPrev*WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
        totalScoreChanges+=countPrev
      } else {
        totalScore+=countPrev*WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
        totalScoreChanges+=countPrev
      }
    } else if(a.isWildcard(prevValueB)){
      if(transitionSetB.contains(ValueTransition(prevValueA,prevValueA))){
        totalScore+=countPrev*WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
        totalScoreChanges+=countPrev
      } else {
        totalScore+=countPrev*WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
        totalScoreChanges+=countPrev
      }
    } else {
      totalScore+=countPrev*SYNCHRONOUS_NON_WILDCARD_NON_CHANGE_TRANSITION
      totalScoreChanges+=countPrev
    }
  }

  def score():Double = {
    if(!totalScore.isNegInfinity){
      if(!(totalScore>=0.0 && totalScore<=1.0))
        println()
      assert(totalScore>=0.0 && totalScore<=1.0)
      assert(totalScoreChanges==totalTransitionCount)
    }
    totalScore
  }

}
object MultipleEventWeightScoreComputer extends StrictLogging {

  logger.error("This class uses IOService standard dates - make sure those are set correctly!")

  def scoreOfSingletonVertex = 0.0
  def scoreForInconsistent = Double.NegativeInfinity
}
