package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.MetadataBasedHeuristicMatchCalculator

import scala.collection.mutable

class MatchCandidateGraph(unmatchedAssociations: mutable.HashSet[SynthesizedTemporalDatabaseTable],
                         heuristicMatchCalulator:MetadataBasedHeuristicMatchCalculator) {

  //we need to distinguish between sure matches and heuristic matches
  val computedMatches = mutable.TreeMap[Int,ComputedMatch]()
  val heuristicMatches = mutable.TreeMap[Int,mutable.ArrayBuffer[HeuristicMatch]]()
  initHeuristicMatches()


  def initHeuristicMatches() = {
    val unmatchedList = unmatchedAssociations.toIndexedSeq
    for(i <- 0 until unmatchedList.size){
      for(j <- (i+1) until unmatchedList.size){
        val curMatch = heuristicMatchCalulator.calculateMatch(unmatchedList(i),unmatchedList(j))
        if(curMatch.score!=0)
          heuristicMatches.getOrElseUpdate(curMatch.score,mutable.ArrayBuffer()).addOne(curMatch)
      }
    }
  }

  def calculateNextBestHeuristicMatch() = {
    //get best heuristic match and update the collection:
    val topHeuristicMatches = heuristicMatches(heuristicMatches.lastKey)
    assert(topHeuristicMatches.size>0)
    val topHeuristicMatch = topHeuristicMatches.remove(topHeuristicMatches.size-1)
    if(topHeuristicMatches.size==0)
      heuristicMatches.remove(heuristicMatches.lastKey)
    //compute the match:
    val computedMatch:Option[ComputedMatch] = topHeuristicMatch.firstMatchPartner.computeUnionMatch(topHeuristicMatch.secondMatchPartner)
    heuristicMatches.remove(heuristicMatches.lastKey)
    if(computedMatch.isDefined){
      computedMatches.put(computedMatch.get.score,computedMatch.get)
    }
  }

  def getTopMatch() = computedMatches(computedMatches.lastKey)

  def noComputedMatchAvailable = computedMatches.isEmpty

}
