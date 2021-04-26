package de.hpi.tfm.fact_merging.metrics

import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{ChangePoint, CommonPointOfInterestIterator, ValueTransition}
import de.hpi.tfm.io.IOService
import de.hpi.tfm.util.MathUtil

import java.time.LocalDate

class MutualInformationComputer[A](a: TemporalFieldTrait[A], b: TemporalFieldTrait[A], numPointsInTime:Int = IOService.STANDARD_TIME_RANGE_SIZE,timeFrameEnd:LocalDate = IOService.STANDARD_TIME_FRAME_END) {

  assert(a.tryMergeWithConsistent(b).isDefined)
  var transitionPairOccurrences = scala.collection.mutable.HashMap[(ValueTransition[Any],ValueTransition[Any]),Int]()
  val transitionOccurrencesA = scala.collection.mutable.HashMap[ValueTransition[Any],Int]()
  val transitionOccurrencesB = scala.collection.mutable.HashMap[ValueTransition[Any],Int]()
  var wcCounter = 0

  def updateCount(valueTransitionA: ValueTransition[Any], valueTransitionB: ValueTransition[Any], count: Int) = {
    if(count>0){
      val prevCount = transitionPairOccurrences.getOrElse((valueTransitionA,valueTransitionB),0)
      transitionPairOccurrences((valueTransitionA,valueTransitionB)) = prevCount + count
      val prevA = transitionOccurrencesA.getOrElse(valueTransitionA,0)
      transitionOccurrencesA(valueTransitionA)=prevA + count
      val prevB = transitionOccurrencesB.getOrElse(valueTransitionB,0)
      transitionOccurrencesB(valueTransitionB)=prevB + count
    }
  }

  def handleWC(value: A) = {
    if(a.isWildcard(value)){
      val res = value + s"_$wcCounter"
      wcCounter+=1
      res
    } else
      value
  }

  def addTransitionCount(valueAPrev: A, valueA: A, valueBPrev: A, valueB:A, count: Int) = {
    //TODO: if none of those are wildcards, we can call it directly:
    if(Set(valueA,valueAPrev,valueB,valueBPrev).forall(!a.isWildcard(_))){
      updateCount(ValueTransition(valueAPrev,valueA),ValueTransition(valueBPrev,valueB),count.toInt)
    } else {
      (0 until count).foreach(i => {
        val transition1 = ValueTransition(handleWC(valueAPrev), handleWC(valueA))
        val transition2 = ValueTransition(handleWC(valueBPrev), handleWC(valueB))
        updateCount(transition1, transition2, 1)
      })
    }
  }

  def mutualInfo():Double = {
    buildTransitionHistogram()
//    println("--------------------------------------------------------------------")
//    transitionPairOccurrences.foreach(println)
//    println("--------------------------------------------------------------------")
//    println("A")
//    transitionOccurrencesA.foreach(println)
//    println("--------------------------------------------------------------------")
//    println("B")
//    transitionOccurrencesB.foreach(println)
//    println("--------------------------------------------------------------------")
    if(!(transitionPairOccurrences.values.sum == numPointsInTime-1))
      println()
    assert(transitionPairOccurrences.values.sum == numPointsInTime-1)
    val denominator = (numPointsInTime-1).toDouble
    val terms = transitionPairOccurrences.map{case ((tA,tB),count) => {
      val pAB = count / denominator
      val pA = transitionOccurrencesA(tA) / denominator
      val pB = transitionOccurrencesB(tB) / denominator
      val term = pAB * MathUtil.log2(pAB / (pA*pB))
      term
    }}
    val res = terms.sum
    if(res.isNaN) {
      println()
    }
    assert(!res.isNaN)
    res
  }

  private def buildTransitionHistogram() = {
    val commonPointOfInterestIterator = new CommonPointOfInterestIterator(a, b)
    var lastPointOfInterest:ChangePoint[A] = null
    commonPointOfInterestIterator.foreach(cp => {
      //handle previous transitions:
      val countPrev = cp.pointInTime.toEpochDay - cp.prevPointInTime.toEpochDay - 1
      val prevValueA = cp.prevValueA
      val prevValueB = cp.prevValueB
      addTransitionCount(prevValueA, prevValueA, prevValueB, prevValueB, countPrev.toInt)
      addTransitionCount(prevValueA, cp.curValueA, prevValueB, cp.curValueB, 1)
      //integrity constraint:
      lastPointOfInterest = cp
    })
    if(lastPointOfInterest.pointInTime!=timeFrameEnd){
      //add last value to
      val count = timeFrameEnd.toEpochDay - lastPointOfInterest.pointInTime.toEpochDay
      addTransitionCount(lastPointOfInterest.curValueA,lastPointOfInterest.curValueA,lastPointOfInterest.curValueB,lastPointOfInterest.curValueB,count.toInt)
    }
  }

}
