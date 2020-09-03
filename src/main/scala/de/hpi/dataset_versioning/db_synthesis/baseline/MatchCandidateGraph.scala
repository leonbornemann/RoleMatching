package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.{MetadataBasedHeuristicMatchCalculator, DataBasedMatchCalculator, TemporalDatabaseTableTrait}
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class MatchCandidateGraph(unmatchedAssociations: mutable.HashSet[SynthesizedTemporalDatabaseTableSketch],
                         heuristicMatchCalulator:DataBasedMatchCalculator) extends StrictLogging{

  def updateGraphAfterMatchExecution(executedMatch: TableUnionMatch[Int],
                                     synthTable: SynthesizedTemporalDatabaseTable,
                                     unionedTableSketch: SynthesizedTemporalDatabaseTableSketch): Unit = {
    //remove both elements in best match and update all pointers to the new table
    removeAndDeleteIfEmpty(executedMatch)
    var scoresToDelete = mutable.HashSet[Int]()
    var matchesToRecompute = mutable.HashSet[TemporalDatabaseTableTrait[Int]]()
    val outdatedMatches = mutable.HashSet[(Int,TableUnionMatch[Int])]()
    curMatches.foreach{case (score,matches) => {
      val overlappingMatches = matches.filter(m => m.hasParterOverlap(executedMatch))
      matchesToRecompute ++= overlappingMatches.map(m => m.getNonOverlappingElement(executedMatch))
      outdatedMatches ++= overlappingMatches.map(m => (score,m))
      if(overlappingMatches.size==matches.size)
        scoresToDelete += score
    }}
    outdatedMatches.foreach{case (score,m) => curMatches(score).remove(m)}
    scoresToDelete.foreach(s => curMatches.remove(s))
    matchesToRecompute.foreach(toRecompute => {
      val curMatch = heuristicMatchCalulator.calculateMatch(toRecompute,unionedTableSketch)
      if(curMatch.score!=0)
        curMatches.getOrElseUpdate(curMatch.score,mutable.HashSet()).addOne(curMatch)
    })
    assert(curMatches.forall(_._2.forall(m => {
      !m.hasParterOverlap(executedMatch)
    })))
  }

  def removeMatch(bestMatch: TableUnionMatch[Int]): Unit = {
    removeAndDeleteIfEmpty(bestMatch)
  }

  private def removeAndDeleteIfEmpty(bestMatch: TableUnionMatch[Int]) = {
    val set = curMatches(bestMatch.score)
    set.remove(bestMatch)
    if (set.isEmpty) {
      curMatches.remove(bestMatch.score)
    }
  }

  def isEmpty = curMatches.isEmpty

  //we need to distinguish between sure matches and heuristic matches
  val curMatches = mutable.TreeMap[Int,mutable.HashSet[TableUnionMatch[Int]]]()
  initHeuristicMatches()
  logger.debug("Finished Heuristic Match calculation")


  def initHeuristicMatches() = {
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
  }

}
object MatchCandidateGraph extends StrictLogging{
  logger.debug("Potential Optimization: MatchCandidateGraph initializes by doing a simple quadratic strategy for ranking the table matches - this will likely be too slow in the future")
  logger.debug("Potential Optimization: MatchCandidateGraph is using a scan through the graph to update it (method: updateGraphAfterMatchExecution), this takes linear time in the number of matches every time the method is called - this might be too slow in practice")

}
