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

}
