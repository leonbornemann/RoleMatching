package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.InitialMatchinStrategy.{INDEX_BASED, InitialMatchinStrategy, NAIVE_PAIRWISE}
import de.hpi.dataset_versioning.db_synthesis.baseline.config.InitialMatchinStrategy
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch, SynthesizedTemporalDatabaseTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.index.MostDistinctTimestampIndexBuilder
import de.hpi.dataset_versioning.db_synthesis.sketches.table.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class MatchCandidateGraph(unmatchedAssociations: mutable.HashSet[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch],
                          heuristicMatchCalulator:DataBasedMatchCalculator,
                          initialMatchingStrategy:InitialMatchinStrategy = InitialMatchinStrategy.INDEX_BASED) extends StrictLogging{

  def updateGraphAfterMatchExecution(executedMatch: TableUnionMatch[Int],
                                     synthTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation,
                                     unionedTableSketch: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch): Unit = {
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
    if(initialMatchingStrategy==INDEX_BASED){
      initMatchesIndexBased
    } else {
      assert(initialMatchingStrategy==NAIVE_PAIRWISE)
      initMatchesNaivePairwise
    }
  }

  private def initMatchesNaivePairwise = {
    val unmatchedList = unmatchedAssociations.toIndexedSeq
    var nMatchesComputed = 0
    for (i <- 0 until unmatchedList.size) {
      val firstMatchPartner = unmatchedList(i)
      for (j <- (i + 1) until unmatchedList.size) {
        val secondMatchPartner = unmatchedList(j)
        val curMatch = heuristicMatchCalulator.calculateMatch(firstMatchPartner, secondMatchPartner)
        nMatchesComputed+=1
        if (curMatch.score != 0)
          curMatches.getOrElseUpdate(curMatch.score, mutable.HashSet()).addOne(curMatch)
      }
    }
    logger.debug(s"Finished Naive Pairwise initial matching, resulting in ${nMatchesComputed} checked matches, of which ${curMatches.map(_._2.size).sum} have a score > 0")
  }

  def getCorrectlyOrderedPair(tuple1: (TemporalDatabaseTableTrait[Int], Iterable[Int]), tuple2: (TemporalDatabaseTableTrait[Int], Iterable[Int])) = {
    assert(tuple1._1.getUnionedOriginalTables.head.compositeID != tuple2._1.getUnionedOriginalTables.head.compositeID)
    if(tuple1._1.getUnionedOriginalTables.head.compositeID < tuple2._1.getUnionedOriginalTables.head.compositeID){
      (tuple1,tuple2)
    } else{
      (tuple2,tuple1)
    }
  }

  private def initMatchesIndexBased = {
    logger.debug("Starting Index-Based initial match computation")
    val indexBuilder = new MostDistinctTimestampIndexBuilder[Int](unmatchedAssociations.map(_.asInstanceOf[TemporalDatabaseTableTrait[Int]]))
    val index = indexBuilder.buildTableIndexOnNonKeyColumns()
    val it = index.tupleGroupIterator
    val computedMatches = mutable.HashSet[(TemporalDatabaseTableTrait[Int],TemporalDatabaseTableTrait[Int])]()
    var nMatchesComputed = 0
    it.foreach(g => {
      val groupsWithTupleIndcies = g.groupMap(t => t._1)(t => t._2).toIndexedSeq
      for (i <- 0 until groupsWithTupleIndcies.size) {
        for (j <- (i + 1) until groupsWithTupleIndcies.size) {
          val ((firstMatchPartner,_),(secondMatchPartner,_)) = getCorrectlyOrderedPair(groupsWithTupleIndcies(i),groupsWithTupleIndcies(j))
          if(!computedMatches.contains((firstMatchPartner,secondMatchPartner))){
            val curMatch = heuristicMatchCalulator.calculateMatch(firstMatchPartner, secondMatchPartner)
            nMatchesComputed+=1
            computedMatches.addOne((firstMatchPartner,secondMatchPartner))
            if(curMatch.score!=0) {
              curMatches.getOrElseUpdate(curMatch.score, mutable.HashSet()).addOne(curMatch)
            }
          }
        }
      }
    })
    logger.debug(s"Finished Index-Based initial matching, resulting in ${nMatchesComputed} checked matches, of which ${curMatches.map(_._2.size).sum} have a score > 0")

  }

  def getNextBestHeuristicMatch() = {
    //get best heuristic match and update the collection:
    val topHeuristicMatches = curMatches(curMatches.lastKey)
    topHeuristicMatches.head
  }

}
object MatchCandidateGraph extends StrictLogging{
  logger.debug("Potential Optimization: MatchCandidateGraph is using a scan through the graph to update it (method: updateGraphAfterMatchExecution), this takes linear time in the number of matches every time the method is called - this might be too slow in practice")

}
