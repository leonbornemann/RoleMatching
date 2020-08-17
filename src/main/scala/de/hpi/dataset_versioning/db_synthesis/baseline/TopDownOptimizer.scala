package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable

import scala.collection.mutable

class TopDownOptimizer(associations: IndexedSeq[DecomposedTemporalTable]) {

  assert(associations.forall(_.isAssociation))

  private val unmatchedAssociations = mutable.HashSet() ++ associations
  private val synthesizedTables = mutable.HashSet[SynthesizedTemporalDatabaseTable]()
  private val matchCandidateGraph = new MatchCandidateGraph(unmatchedAssociations,synthesizedTables)


  val MIN_TOP_MATCH_Score = 10 //TODO: this is arbitrary, we need to tune this!
  val MAX_NUM_TRIES_PER_ITERATION = 100

  def optimize() = {

    while(!unmatchedAssociations.isEmpty){
      //explore a few of the heuristically promosing candidates
      var numTries = 0
      while((matchCandidateGraph.noComputedMatchAvailable ||  matchCandidateGraph.getTopMatch().score < MIN_TOP_MATCH_Score)
        && numTries<MAX_NUM_TRIES_PER_ITERATION){
        matchCandidateGraph.calculateNextBestHeuristicMatch()
      }
    }
  }


}
