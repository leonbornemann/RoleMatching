package de.hpi.tfm.evaluation.wikipediaStyle

import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{CommonPointOfInterestIterator, FactLineage}

import java.time.LocalDate

abstract class StatComputer {

  def getEvidenceInTestPhase(v1: TemporalFieldTrait[Any], v2: TemporalFieldTrait[Any], trainTimeEnd:LocalDate) = {
    val evidence = new CommonPointOfInterestIterator(v1,v2)
      .withFilter(cp => cp.pointInTime.isAfter(trainTimeEnd))
      .toIndexedSeq
      .map{cp => {
        if(!FactLineage.isWildcard(cp.curValueA) && !FactLineage.isWildcard(cp.curValueB)) 1 else 0
      }}
      .sum
    evidence
  }

  //Dirty: copied from HoldoutTimeEvaluator
  def getPointInTimeOfRealChangeAfterTrainPeriod(lineage: TemporalFieldTrait[Any],trainTimeEnd:LocalDate) = {
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
