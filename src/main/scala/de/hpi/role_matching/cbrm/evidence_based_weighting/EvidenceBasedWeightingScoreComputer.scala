package de.hpi.role_matching.cbrm.evidence_based_weighting

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.{ChangePoint, CommonPointOfInterestIterator, RoleLineage, ValueTransition}
import de.hpi.role_matching.cbrm.evidence_based_weighting.EventOccurrenceStatistics.{NEUTRAL, STRONGNEGATIVE, STRONGPOSTIVE, WEAKNEGATIVE, WEAKPOSTIVE}
import de.hpi.role_matching.cbrm.evidence_based_weighting.EvidenceBasedWeightingScoreComputer.{getCountPrev, transitionIsNonInformative}

import java.time.LocalDate

class EvidenceBasedWeightingScoreComputer(a:RoleLineage,
                                             b:RoleLineage,
                                             val TIMESTAMP_GRANULARITY_IN_DAYS:Int,
                                             timeEnd:LocalDate, // this should be the end of train time!
                                             nonInformativeValues:Set[Any],
                                             nonInformativeValueIsStrict:Boolean, //true if it is enough for one value in a transition to be non-informative to discard it, false if both of them need to be non-informative to discard it
                                             transitionHistogramForTFIDF:Option[Map[ValueTransition,Int]],
                                             lineageCount:Option[Int]
                                         ) {

  val totalTransitionCount = (GLOBAL_CONFIG.STANDARD_TIME_FRAME_START.toEpochDay to timeEnd.toEpochDay by TIMESTAMP_GRANULARITY_IN_DAYS).size-1
  val WILDCARD_TO_KNOWN_TRANSITION_WEIGHT = -0.1 / totalTransitionCount
  val WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT = -0.5 / totalTransitionCount
  val BOTH_WILDCARD_WEIGHT = 0
  val SYNCHRONOUS_NON_INFORMATIVE_TRANSITION_WEIGHT = 0

  def exponentialFrequency(x: Double) = {
    val a = 0.0000001
    val y = (Math.pow(a,x)-1) / (a-1).toDouble
    y
  }

  def getWeightedTransitionScore(d: Double, t: ValueTransition) = {
    if(transitionHistogramForTFIDF.isDefined){
      val weight = 1.0 / (transitionHistogramForTFIDF.get(t) - 1)
      weight*(d / totalTransitionCount)
    } else {
      d / totalTransitionCount
    }
  }

  def SYNCHRONOUS_NON_WILDCARD_CHANGE_TRANSITION_WEIGHT(t:ValueTransition) = {
    getWeightedTransitionScore(0.5,t)

  }
  def SYNCHRONOUS_NON_WILDCARD_NON_CHANGE_TRANSITION_WEIGHT(t:ValueTransition) = {
    getWeightedTransitionScore(0.1,t)
  }

  val transitionSetA = a.valueTransitions(true,false)
  val transitionSetB = b.valueTransitions(true,false)
  var totalScore = 0.5
  var totalScoreChanges =0

  computeScore()

  private def computeScore() = {
    if(!a.tryMergeWithConsistent(b).isDefined){
      totalScore = EvidenceBasedWeightingScoreComputer.scoreForInconsistent
    } else {
      val commonPointOfInterestIterator = new CommonPointOfInterestIterator(a,b)
      commonPointOfInterestIterator
        .withFilter(cp => !cp.pointInTime.isAfter(timeEnd))
        .foreach(cp => {
          //handle previous transitions:
          val countPrev = getCountPrev(cp,TIMESTAMP_GRANULARITY_IN_DAYS,None)
          val prevValueA = cp.prevValueA
          val prevValueB = cp.prevValueB
          handleSameValueTransitions(prevValueA,prevValueB,countPrev.toInt)
          handleCurrentValueTransition(cp)
          totalScoreChanges+=1
        })
      val lastKey = Seq(a.getValueLineage.maxBefore(timeEnd.plusDays(1)).get._1,b.getValueLineage.maxBefore(timeEnd.plusDays(1)).get._1).maxBy(_.toEpochDay)
      val lastValueA = a.getValueLineage.last._2
      val lastValueB = b.getValueLineage.last._2
      val countLastInDays = timeEnd.toEpochDay - lastKey.toEpochDay
      assert(countLastInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0)
      val countLast = countLastInDays / TIMESTAMP_GRANULARITY_IN_DAYS
      handleSameValueTransitions(lastValueA,lastValueB,countLast.toInt)
    }
  }

  private def handleCurrentValueTransition(cp: ChangePoint) = {
    val values = Set(cp.prevValueA, cp.prevValueB, cp.curValueA, cp.curValueB)
    //handle transition:
    val noWildcardInTransition = values.forall(v => !a.isWildcard(v))
    if (noWildcardInTransition) {
      assert(cp.prevValueA == cp.prevValueB && cp.curValueA == cp.curValueB)
      if (transitionIsNonInformative(ValueTransition(cp.prevValueA, cp.curValueA), nonInformativeValues, nonInformativeValueIsStrict)) {
        totalScore += 1 * SYNCHRONOUS_NON_INFORMATIVE_TRANSITION_WEIGHT
      } else {
        totalScore += 1 * SYNCHRONOUS_NON_WILDCARD_CHANGE_TRANSITION_WEIGHT(ValueTransition(cp.prevValueA, cp.curValueA))
      }
    } else {
      val aChanged = cp.curValueA != cp.prevValueA && !a.isWildcard(cp.curValueA) && !a.isWildcard(cp.prevValueA)
      val bChanged = cp.curValueB != cp.prevValueB && !a.isWildcard(cp.curValueB) && !a.isWildcard(cp.prevValueB)
      if (aChanged) {
        if (transitionSetB.contains(ValueTransition(cp.prevValueA, cp.curValueA))) {
          totalScore += 1 * WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
        } else {
          totalScore += 1 * WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
        }
      } else if (bChanged) {
        if (transitionSetA.contains(ValueTransition(cp.prevValueB, cp.curValueB))) {
          totalScore += 1 * WILDCARD_TO_KNOWN_TRANSITION_WEIGHT
        } else {
          totalScore += 1 * WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
        }
      } else {
        totalScore += 1 * WILDCARD_TO_UNKNOWN_TRANSITION_WEIGHT
      }
    }
  }

  def handleSameValueTransitions(prevValueA: Any, prevValueB: Any, countPrev: Int) = {
    if(countPrev!=0){
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
        assert(prevValueA==prevValueB)
        val t = ValueTransition(prevValueA,prevValueB)
        if(transitionIsNonInformative(t,nonInformativeValues,nonInformativeValueIsStrict)){
          totalScore+=countPrev*SYNCHRONOUS_NON_INFORMATIVE_TRANSITION_WEIGHT
        } else{
          totalScore+=countPrev*SYNCHRONOUS_NON_WILDCARD_NON_CHANGE_TRANSITION_WEIGHT(t)
        }
      }
      totalScoreChanges+=countPrev
    }
  }

  def score():Double = {
    if(!totalScore.isNegInfinity){
      if(!(totalScore>=0.0 && totalScore<=1.0))
        println()
      assert(totalScore>=0.0 && totalScore<=1.0)
      if(totalScoreChanges!=totalTransitionCount){
        println()
      }
      assert(totalScoreChanges==totalTransitionCount)
    }
    totalScore
  }

}
object EvidenceBasedWeightingScoreComputer extends StrictLogging {

  def getCountPrev(cp: ChangePoint,TIMESTAMP_GRANULARITY_IN_DAYS:Int,trainTimeEnd:Option[LocalDate]) = {
    assert(trainTimeEnd.isEmpty || !trainTimeEnd.get.isBefore(cp.prevPointInTime))
    val end = if(trainTimeEnd.isDefined && trainTimeEnd.get.isBefore(cp.pointInTime)) trainTimeEnd.get else cp.pointInTime
    val countPrevInDays = end.toEpochDay - cp.prevPointInTime.toEpochDay - TIMESTAMP_GRANULARITY_IN_DAYS
    if(!(countPrevInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0))
      println()
    assert(countPrevInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0)
    val countPrev = countPrevInDays / TIMESTAMP_GRANULARITY_IN_DAYS
    countPrev
  }



  def getCountForSameValueTransition(prevValueA: Any,
                                        prevValueB: Any,
                                        countPrev: Int,
                                        isWildcard:(Any => Boolean),
                                        transitionSetA:Set[ValueTransition],
                                        transitionSetB:Set[ValueTransition],
                                        nonInformativeValues:Set[Any],
                                        nonInformativeValueIsStrict:Boolean,
                                        transitionHistogramForTFIDF:Option[Map[ValueTransition,Int]]=None
                                       ) = {
    val totalScore = new EventOccurrenceStatistics(null)
    var isInvalid = false
    if(countPrev!=0){
      if(isWildcard(prevValueA) && isWildcard(prevValueB)){
        totalScore.addScore(NEUTRAL,countPrev,countPrev)
      } else if(isWildcard(prevValueA)){
        if(transitionSetA.contains(ValueTransition(prevValueB,prevValueB))){
          totalScore.addScore(WEAKNEGATIVE,countPrev,countPrev)
        } else {
          totalScore.addScore(STRONGNEGATIVE,countPrev,countPrev)
        }
      } else if(isWildcard(prevValueB)){
        if(transitionSetB.contains(ValueTransition(prevValueA,prevValueA))){
          totalScore.addScore(WEAKNEGATIVE,countPrev,countPrev)
        } else {
          totalScore.addScore(STRONGNEGATIVE,countPrev,countPrev)
        }
      } else {
        if(prevValueA==prevValueB){
          val t = ValueTransition(prevValueA,prevValueB)
          if(transitionIsNonInformative(t,nonInformativeValues,nonInformativeValueIsStrict)){
            totalScore.addScore(NEUTRAL,countPrev,countPrev)
          } else{
            if(!transitionHistogramForTFIDF.get.contains(t)){
              println()
            }
            val score = 1.0f / (transitionHistogramForTFIDF.get(t) - 1)
            totalScore.addScore(WEAKPOSTIVE,countPrev,score)
          }
        } else {
          isInvalid=true
          //TODO: investigate this!
          //Nothin happens -- this was an invalid match
        }
      }
    }
    if(isInvalid)
      None
    else
      Some(totalScore)
  }

  def getCountForTransition(cp: ChangePoint,
                            isWildcard:(Any => Boolean),
                            transitionSetA:Set[ValueTransition],
                            transitionSetB:Set[ValueTransition],
                            nonInformativeValues:Set[Any],
                            nonInformativeValueIsStrict:Boolean,
                            transitionHistogramForTFIDF:Option[Map[ValueTransition,Int]]=None) = {
    val values = Set(cp.prevValueA, cp.prevValueB, cp.curValueA, cp.curValueB)
    //handle transition:
    val noWildcardInTransition = values.forall(v => !isWildcard(v))
    val totalScore = new EventOccurrenceStatistics(null)
    var invalid=false
    if (noWildcardInTransition) {
      if(cp.prevValueA == cp.prevValueB && cp.curValueA == cp.curValueB){
        if (transitionIsNonInformative(ValueTransition(cp.prevValueA, cp.curValueA), nonInformativeValues, nonInformativeValueIsStrict)) {
          totalScore.addScore(NEUTRAL,1,1)
        } else {
          val score = 1.0f / (transitionHistogramForTFIDF.get(ValueTransition(cp.prevValueA,cp.curValueA)) - 1)
          totalScore.addScore(STRONGPOSTIVE,1,score)
        }
      } else {
        invalid=true
        //nothing happens this was an invalid match
      }
    } else {
      val aChanged = cp.curValueA != cp.prevValueA && !isWildcard(cp.curValueA) && !isWildcard(cp.prevValueA)
      val bChanged = cp.curValueB != cp.prevValueB && !isWildcard(cp.curValueB) && !isWildcard(cp.prevValueB)
      if (aChanged) {
        if (transitionSetB.contains(ValueTransition(cp.prevValueA, cp.curValueA))) {
          totalScore.addScore(WEAKNEGATIVE,1,1)
        } else {
          totalScore.addScore(STRONGNEGATIVE,1,1)
        }
      } else if (bChanged) {
        if (transitionSetA.contains(ValueTransition(cp.prevValueB, cp.curValueB))) {
          totalScore.addScore(WEAKNEGATIVE,1,1)
        } else {
          totalScore.addScore(STRONGNEGATIVE,1,1)
        }
      } else {
        totalScore.addScore(STRONGNEGATIVE,1,1)
      }
    }
    if(invalid)
      None
    else
      Some(totalScore)
  }

  def transitionIsNonInformative(value: ValueTransition,
                                    nonInformativeValues:Set[Any],
                                    nonInformativeValueIsStrict:Boolean): Boolean = {
    val containsPrev = nonInformativeValues.contains(value.prev)
    val containsAfter = nonInformativeValues.contains(value.after)
    if(nonInformativeValueIsStrict) containsPrev || containsAfter else containsPrev && containsAfter
  }


  logger.error("This class uses IOService standard dates - make sure those are set correctly!")

  def scoreOfSingletonVertex = 0.0
  def scoreForInconsistent = Double.NegativeInfinity
}
