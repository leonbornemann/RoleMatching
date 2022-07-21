package de.hpi.role_matching.data

import de.hpi.util.GLOBAL_CONFIG

import java.time.LocalDate

class CommonPointOfInterestIterator(a: RoleLineage, b: RoleLineage) extends Iterator[ChangePoint]{
  val vlA = a.getValueLineage.iterator
  val vlB = b.getValueLineage.iterator
  val startA = vlA.next()
  val startB = vlB.next()
  var curElemA = vlA.nextOption()
  var curElemB = vlB.nextOption()
  var prevElemA:Any = startA._2
  var prevElemB:Any = startB._2
  assert(startA._1==startB._1 && startA._1==GLOBAL_CONFIG.STANDARD_TIME_FRAME_START)
  var prevTimepoint:LocalDate = GLOBAL_CONFIG.STANDARD_TIME_FRAME_START

  override def hasNext: Boolean = curElemA.isDefined || curElemB.isDefined

  def curValueA: Any = {
    if(!curElemA.isDefined || curElemA.get._1.isAfter(curTimepoint))
      prevElemA
    else
      curElemA.get._2
  }

  def curValueB: Any = {
    if(!curElemB.isDefined || curElemB.get._1.isAfter(curTimepoint))
      prevElemB
    else
      curElemB.get._2
  }

  override def next(): ChangePoint = {
    //val curValueA = if(!curElemA.isDefined) prevElemA else curElemA.get._2
    //val curValueB = if(!curElemB.isDefined) prevElemB else curElemB.get._2
    val toReturn = ChangePoint(prevElemA,prevElemB,curValueA,curValueB,curTimepoint,prevTimepoint,true)
    prevTimepoint = curTimepoint
    if(curElemA.isDefined && prevTimepoint == curElemA.get._1){
      //advance A
      advanceA()
    }
    if(curElemB.isDefined && prevTimepoint == curElemB.get._1){
      //advance B
      advanceB()
    }
    if(!hasNext)
      toReturn.isLast = true
    else
      toReturn.isLast = false
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


