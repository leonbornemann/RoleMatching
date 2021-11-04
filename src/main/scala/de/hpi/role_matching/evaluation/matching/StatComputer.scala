package de.hpi.role_matching.evaluation.matching


import de.hpi.role_matching.cbrm.data.{CommonPointOfInterestIterator, RoleLineage}

import java.time.LocalDate

abstract class StatComputer {

  def getEvidenceInTestPhase(v1: RoleLineage, v2: RoleLineage, trainTimeEnd:LocalDate) = {
    val evidence = new CommonPointOfInterestIterator(v1,v2)
      .withFilter(cp => cp.pointInTime.isAfter(trainTimeEnd))
      .toIndexedSeq
      .map{cp => {
        if(!RoleLineage.isWildcard(cp.curValueA) && !RoleLineage.isWildcard(cp.curValueB)) 1 else 0
      }}
      .sum
    evidence
  }

  def getEvidenceInTrainPhase(v1: RoleLineage, v2: RoleLineage, trainTimeEnd:LocalDate) = {
    val evidence = new CommonPointOfInterestIterator(v1,v2)
      .withFilter(cp => cp.pointInTime.isBefore(trainTimeEnd))
      .toIndexedSeq
      .map{cp => {
        if(!RoleLineage.isWildcard(cp.curValueA) && !RoleLineage.isWildcard(cp.curValueB)) 1 else 0
      }}
      .sum
    evidence
  }

  //Dirty: copied from HoldoutTimeEvaluator
  def getPointInTimeOfRealChangeAfterTrainPeriod(lineage: RoleLineage,trainTimeEnd:LocalDate) = {
    val prevNonWcValue = lineage.getValueLineage.filter(t => !lineage.isWildcard(t._2) && !t._1.isAfter(trainTimeEnd)).lastOption
    if(prevNonWcValue.isEmpty)
      None
    else {
      val it = lineage.getValueLineage.iteratorFrom(trainTimeEnd)
      var pointInTime:Option[LocalDate] = None
      while(it.hasNext && !pointInTime.isDefined){
        val (curTIme,curValue) = it.next()
        if(!lineage.isWildcard(curValue) && curValue!=prevNonWcValue.get._2){
          pointInTime = Some(curTIme)
        }
      }
      pointInTime
    }
  }

}
