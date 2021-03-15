package de.hpi.dataset_versioning.db_synthesis.sketches.field

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.ValueTransition
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.MathUtil

import java.time.LocalDate

class MutualInformationComputer[A](a: TemporalFieldTrait[A], b: TemporalFieldTrait[A], numPointsInTime:Int = IOService.STANDARD_TIME_RANGE_SIZE,timeFrameEnd:LocalDate = IOService.STANDARD_TIME_FRAME_END) {

  assert(a.tryMergeWithConsistent(b).isDefined)
  var transitionPairOccurrences = scala.collection.mutable.HashMap[(ValueTransition,ValueTransition),Int]()
  val transitionOccurrencesA = scala.collection.mutable.HashMap[ValueTransition,Int]()
  val transitionOccurrencesB = scala.collection.mutable.HashMap[ValueTransition,Int]()
  var wcCounter = 0

  def updateCount(valueTransitionA: ValueTransition, valueTransitionB: ValueTransition, count: Int) = {
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
    var lastPointOfInterest:ChangePoint = null
    commonPointOfInterestIterator.foreach(cp => {
      //handle previous transitions:
      val countPrev = cp.pointInTime.toEpochDay - cp.prevPointInTime.toEpochDay - 1
      val prevValueA = cp.prevValue1
      val prevValueB = cp.prevValue2
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

  class CommonPointOfInterestIterator(a: TemporalFieldTrait[A], b: TemporalFieldTrait[A]) extends Iterator[ChangePoint]{
    val vlA = a.getValueLineage.iterator
    val vlB = b.getValueLineage.iterator
    val startA = vlA.next()
    val startB = vlB.next()
    var curElemA = vlA.nextOption()
    var curElemB = vlB.nextOption()
    var prevElemA:A = startA._2
    var prevElemB:A = startB._2
    assert(startA._1==startB._1 && startA._1==IOService.STANDARD_TIME_FRAME_START)
    var prevTimepoint:LocalDate = IOService.STANDARD_TIME_FRAME_START

    override def hasNext: Boolean = curElemA.isDefined || curElemB.isDefined

    def curValueA: A = {
      if(!curElemA.isDefined || curElemA.get._1.isAfter(curTimepoint))
        prevElemA
      else
        curElemA.get._2
    }

    def curValueB: A = {
      if(!curElemB.isDefined || curElemB.get._1.isAfter(curTimepoint))
        prevElemB
      else
        curElemB.get._2
    }

    override def next(): ChangePoint = {
      //val curValueA = if(!curElemA.isDefined) prevElemA else curElemA.get._2
      //val curValueB = if(!curElemB.isDefined) prevElemB else curElemB.get._2
      val toReturn = ChangePoint(prevElemA,prevElemB,curValueA,curValueB,curTimepoint,prevTimepoint)
      prevTimepoint = curTimepoint
      if(curElemA.isDefined && prevTimepoint == curElemA.get._1){
        //advance A
        advanceA()
      }
      if(curElemB.isDefined && prevTimepoint == curElemB.get._1){
        //advance B
        advanceB()
      }
      toReturn
    }

    def curTimepoint = {
      if(!curElemB.isDefined)
        curElemA.get._1
      else if(!curElemA.isDefined)
        curElemB.get._1
      else if(curElemA.get._1.isBefore(curElemB.get._1))
        curElemA.get._1
      else
        curElemB.get._1
    }

    private def advanceA() = {
      prevElemA = curElemA.get._2
      curElemA = vlA.nextOption()
    }

    private def advanceB() = {
      prevElemB = curElemB.get._2
      curElemB = vlB.nextOption()
    }
  }

  case class ChangePoint(prevValue1:A, prevValue2:A, curValueA:A, curValueB:A, pointInTime:LocalDate, prevPointInTime:LocalDate) {
    def firstValueChanged: Boolean = prevValue1!=curValueA &&
      !(a.isWildcard(curValueA) && a.isWildcard(prevValue1))

    def secondValueChanged: Boolean = prevValue2!=curValueB &&
      !(a.isWildcard(curValueB) && a.isWildcard(prevValue2))

  }

}
