package de.hpi.dataset_versioning.db_synthesis.sketches.field

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.ValueTransition
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.MathUtil.log2

import java.lang.AssertionError
import java.time.LocalDate
import scala.collection.mutable

class EntropyComputer[T](field: TemporalFieldTrait[T]) {
  //throw new AssertionError("There is still an unresolved bug in entropy calculation that sometimes leads to NaN - for example for TupleReference(3c9v-pnva.3_2(SK30, _location_longitude),538)")

  val WILDCARD = field.WILDCARDVALUES.toIndexedSeq.sortBy(_.toString)
  val transitions = mutable.HashMap[ValueTransition[Any], Int]()
  var curWCCount = 0

  def entropy: Double = {
    buildTransitionsWildCardUnequalWildcard(vl)
    if(transitions.map(_._2).sum != IOService.STANDARD_TIME_RANGE_SIZE-1){
      println() //OFF by exactly 1
    }
    assert(transitions.map(_._2).sum == IOService.STANDARD_TIME_RANGE_SIZE-1)
    entropy(transitions,IOService.STANDARD_TIME_RANGE_SIZE)
  }

  val vl = field.getValueLineage.toIndexedSeq

  def buildTransitionsWildCardUnequalWildcard(vl:IndexedSeq[(LocalDate,T)]) = {
    for(i<-0 until vl.size){
      val (curArrival,curValue) = vl(i)
      val (nextArrival,nextValue) = if(i+1 != vl.size) vl(i+1) else (IOService.STANDARD_TIME_FRAME_END.plusDays(1),None) //we add plus one so the difference works
      assert(!nextArrival.isBefore(curArrival))
      val sameValueTransitionCount =  nextArrival.toEpochDay - curArrival.toEpochDay -1
      if(sameValueTransitionCount>0)
        handleTransition(curValue,curValue,sameValueTransitionCount.toInt)
      if(i+1 != vl.size) {
        handleTransition(curValue, nextValue, 1)
      }
    }
  }

  def handlePotentialNewWildcardOccurence(value: Any) = {
    if(value.isInstanceOf[T] && field.isWildcard(value.asInstanceOf[T])){
      val toReturn = s"${WILDCARD}_$curWCCount"
      curWCCount+=1
      toReturn
    } else
      value
  }

  private def handleTransition(before: Any, after: Any, toAdd:Int) = {
    if(before!=after) {
      assert(toAdd==1)
      val actualBefore = handlePotentialNewWildcardOccurence(before)
      val actualAfter = handlePotentialNewWildcardOccurence(after)
      addTransitionCount(toAdd, actualBefore, actualAfter)
    } else {
      if(field.isWildcard(before.asInstanceOf[T])){
        val values = (0 to toAdd).map(_ => handlePotentialNewWildcardOccurence(before))
        (0 until values.size-1).foreach(i => {
          val actualBefore = values(i)
          val actualAfter = values(i+1)
          val transition = ValueTransition(actualBefore, actualAfter)
          assert(!transitions.contains(transition))
          transitions(transition) = 1
        })
      } else {
        addTransitionCount(toAdd, before, after)
      }
    }
  }

  private def addTransitionCount(toAdd: Int, actualBefore: Any, actualAfter: Any) = {
    val transition = ValueTransition(actualBefore, actualAfter)
    val existingCount = transitions.getOrElseUpdate(transition, 0)
    transitions(transition) = existingCount + toAdd
  }

  private def entropy(transitions: mutable.HashMap[ValueTransition[Any], Int], lineageSize: Int): Double = {
    val entropy = -transitions.values.map(count => {
      val pXI = count / (lineageSize - 1).toDouble
      pXI * log2(pXI)
    }).sum
    entropy
  }
}
