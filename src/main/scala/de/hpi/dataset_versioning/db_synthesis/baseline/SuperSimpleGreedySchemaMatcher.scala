package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable

import scala.collection.mutable

class SuperSimpleGreedySchemaMatcher(synthesizedTemporalDatabaseTable: SynthesizedTemporalDatabaseTable, curCandidate: DecomposedTemporalTable) {

  val stillMatchableCandidates = mutable.HashSet[AttributeLineageMatchCandidate]() ++ curCandidate.containedAttrLineages.map(new AttributeLineageMatchCandidate(_))

//  def scoreCandidate(c: AttributeLineageMatchCandidate):(AttributeLineageMatchCandidate,Int) = {
//
//  }
//
//  def rankCandidates(candidates:mutable.HashSet[AttributeLineageMatchCandidate]): mutable.IndexedSeq[AttributeLineageMatchCandidate] = {
//    val curCandidateTable = TemporalTable.load(curCandidate.originalID)
//      .project(curCandidate)
//    //Now we can do ranking by value overlap
//    candidates.map(c => scoreCandidate(c))
//      .toIndexedSeq.sortBy(_._2)
//      .map(_._1)
//  }

  def getBestSchemaMatching() = {
    println(s"Matching lineages of size ${(curCandidate.containedAttrLineages.size,synthesizedTemporalDatabaseTable.schema.size)}")
    //TODO: what is the best way of enumerating all matching possibilities
    val dbKey = synthesizedTemporalDatabaseTable.keyAttributeLineages
    val dbNonKey = synthesizedTemporalDatabaseTable.nonKeyAttributeLineages
    val toMatchKey = curCandidate.primaryKey
    val toMatchNonKey = curCandidate.nonKeyAttributeLineages
    assert(dbNonKey.size==1)
    assert(toMatchNonKey.size==1)
    val intersectedActiveTime = synthesizedTemporalDatabaseTable.getActiveTime.intersect(curCandidate.getActiveTime)
    //dbNonKey and toMatchNonKey need to agree during intersected time
    //TODO: this is not enough if we assume the no view definition change semantic! - Then we need to use a slightly different form of activeTime
    //TODO: use intersected Active time
//    if(dbNonKey.head.activeTimeIntervals.isValidMatch(toMatchNonKey.head.activeTimeIntervals)) //TODO: store original schema changes in dtts
//    synthesizedTemporalDatabaseTable.schema.foreach(dbAl => {
//      val activeTimes = dbAl.activeTimeIntervals
//      val nameSet = dbAl.nameSet
//      //we need a legal set of attribute lineages in dtt1, that covers this time period
//      var candidates = stillMatchableCandidates
//        .filter(c => c.al.nameSet.intersect(nameSet).size!=0 && //TODO: we can drop this filter in an improvement
//          activeTimes.isIncludedIn(c.unmatchedTimeIntervals))
//      //now we need some way to prioritize the candidates:
//      val candidatesRanked = rankCandidates(dbAl,candidates)
//      //TODO: we are maybe better off sorting both by start and end time - doing sequence of temporal interval matching
//    })
  }

}
