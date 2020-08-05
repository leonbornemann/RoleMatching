package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable

import scala.collection.mutable

class SuperSimpleGreedySchemaMatcher(synthesizedTemporalDatabaseTable: SynthesizedTemporalDatabaseTable, curCandidate: DecomposedTemporalTable) {

  val stillMatchableCandidates = mutable.HashSet[AttributeLineageMatchCandidate]() ++ curCandidate.containedAttrLineages.map(new AttributeLineageMatchCandidate(_))

  def rankCandidates(candidates:mutable.HashSet[AttributeLineageMatchCandidate]): mutable.IndexedSeq[AttributeLineageMatchCandidate] = {
    val curCandidateTable = TemporalTable.load(curCandidate.originalID)
  }

  def getBestSchemaMatching() = {
    synthesizedTemporalDatabaseTable.schema.foreach(dbAl => {
      val activeTimes = dbAl.activeTimeIntervals
      val nameSet = dbAl.nameSet
      //we need a legal set of attribute lineages in dtt1, that covers this time period
      var candidates = stillMatchableCandidates
        .filter(c => c.al.nameSet.intersect(nameSet).size!=0 && //TODO: we can drop this filter in an improvement
          activeTimes.isIncludedIn(c.unmatchedTimeIntervals))
      //now we need some way to prioritize the candidates:
      val candidatesRanked = rankCandidates(candidates)
      //TODO: we are maybe better off sorting both by start and end time - doing sequence of temporal interval matching
    })
  }


  def getSchemaMatching() = {

  }

}
