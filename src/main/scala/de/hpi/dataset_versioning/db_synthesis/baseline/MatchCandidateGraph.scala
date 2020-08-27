package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.{MetadataBasedHeuristicMatchCalculator, SketchBasedHeuristicMatchCalculator}
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class MatchCandidateGraph(unmatchedAssociations: mutable.HashSet[SynthesizedTemporalDatabaseTableSketch],
                         heuristicMatchCalulator:SketchBasedHeuristicMatchCalculator) extends StrictLogging{

  def updateGraphAfterMatchExecution(executedMatch: TableUnionMatch, synthTable: SynthesizedTemporalDatabaseTable,unionedTableSketch: SynthesizedTemporalDatabaseTableSketch): Unit = {
    logger.debug("using a scan through the graph to update it, this takes linear time every time the method is called - this might be too slow in practice")
    //remove both elements in best match and update all pointers to the new table
    removeAndDeleteIfEmpty(executedMatch)
    var scoresToDelete = mutable.HashSet[Int]()
    var allToRecompute = mutable.HashSet[SynthesizedTemporalDatabaseTableSketch]()
    curMatches.foreach{case (score,matches) => {
      val toRecompute = matches.filter(m => m.hasParterOverlap(executedMatch))
        .map(m => m.getNonOverlappingElement(executedMatch))
      allToRecompute ++= toRecompute
      if(toRecompute.size==matches.size)
        scoresToDelete += score
    }}
    scoresToDelete.foreach(s => curMatches.remove(s))
    allToRecompute.foreach(toRecompute => {
      val curMatch = heuristicMatchCalulator.calculateMatch(toRecompute,unionedTableSketch)
      if(curMatch.score!=0)
        curMatches.getOrElseUpdate(curMatch.score,mutable.HashSet()).addOne(curMatch)
    })
  }

  def removeMatch(bestMatch: TableUnionMatch): Unit = {
    removeAndDeleteIfEmpty(bestMatch)
  }

  private def removeAndDeleteIfEmpty(bestMatch: TableUnionMatch) = {
    val set = curMatches(bestMatch.score)
    set.remove(bestMatch)
    if (set.isEmpty) {
      curMatches.remove(bestMatch.score)
    }
  }

  def isEmpty = curMatches.isEmpty

  val N_HEURISTIC_MATCHES_TO_EXPLORE_PER_ITERATION = 5

  //we need to distinguish between sure matches and heuristic matches
  val computedMatches = mutable.TreeMap[Int,TableUnionMatch]()
  val curMatches = mutable.TreeMap[Int,mutable.HashSet[TableUnionMatch]]()
  val dttToSynthesizedTable = mutable.HashMap[DecomposedTemporalTable,SynthesizedTemporalDatabaseTableIdentifier]()
  initHeuristicMatches()
  logger.debug("Finished Heuristic Match calculation")


  def initHeuristicMatches() = {
    logger.debug("Starting simple quadratic strategy for finding the best table match - this will be too slow in the future")
    val unmatchedList = unmatchedAssociations.toIndexedSeq
    for(i <- 0 until unmatchedList.size){
      for(j <- (i+1) until unmatchedList.size){
        val curMatch = heuristicMatchCalulator.calculateMatch(unmatchedList(i),unmatchedList(j))
        if(curMatch.score!=0)
          curMatches.getOrElseUpdate(curMatch.score,mutable.HashSet()).addOne(curMatch)
      }
    }
  }

  def getNextBestHeuristicMatch() = {
    //get best heuristic match and update the collection:
    val topHeuristicMatches = curMatches(curMatches.lastKey)
    topHeuristicMatches.head
//    assert(topHeuristicMatches.size>0)
//    val topHeuristicMatch = topHeuristicMatches.remove(topHeuristicMatches.size-1)
//    if(topHeuristicMatches.size==0)
//      heuristicMatches.remove(heuristicMatches.lastKey)
//    //compute the match:
//    val computedMatch:Option[TableUnionMatch] = topHeuristicMatch.firstMatchPartner.computeUnionMatch(topHeuristicMatch.secondMatchPartner)
//    heuristicMatches.remove(heuristicMatches.lastKey)
//    if(computedMatch.isDefined){
//      computedMatches.put(computedMatch.get.score,computedMatch.get)
//    }
  }

  def getTopMatch() = computedMatches(computedMatches.lastKey)

  def noComputedMatchAvailable = computedMatches.isEmpty

}
