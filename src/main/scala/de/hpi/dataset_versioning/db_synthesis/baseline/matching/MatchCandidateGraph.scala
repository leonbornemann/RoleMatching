package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.InitialMatchinStrategy.{INDEX_BASED, InitialMatchinStrategy, NAIVE_PAIRWISE}
import de.hpi.dataset_versioning.db_synthesis.baseline.config.{GLOBAL_CONFIG, InitialMatchinStrategy}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.dataset_versioning.db_synthesis.baseline.index.MostDistinctTimestampIndexBuilder

import scala.collection.mutable

class MatchCandidateGraph(unmatchedAssociations: mutable.HashSet[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch],
                          heuristicMatchCalulator:DataBasedMatchCalculator,
                          initialMatchingStrategy:InitialMatchinStrategy = InitialMatchinStrategy.INDEX_BASED) extends StrictLogging{

  def updateGraphAfterMatchExecution(executedMatch: TableUnionMatch[Int],
                                     unionedTableSketch: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch): Unit = {
    //remove both elements in best match and update all pointers to the new table
    removeAndDeleteIfEmpty(executedMatch)
    var scoresToDelete = mutable.HashSet[Float]()
    var matchesToRecompute = mutable.HashSet[TemporalDatabaseTableTrait[Int]]()
    val outdatedMatches = mutable.HashSet[(Float,TableUnionMatch[Int])]()
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
  val curMatches = mutable.TreeMap[Float,mutable.HashSet[TableUnionMatch[Int]]]()
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

  def gaussSUm(n: Int) = n*(n+1) / 2

  private def initMatchesIndexBased = {
    logger.debug("Starting Index-Based initial match computation")
    val indexBuilder = new MostDistinctTimestampIndexBuilder[Int](unmatchedAssociations.map(_.asInstanceOf[TemporalDatabaseTableTrait[Int]]))
    val index = indexBuilder.buildTableIndexOnNonKeyColumns()
    val it = index.tupleGroupIterator
    //index.serializeDetailedStatistics()
    if(GLOBAL_CONFIG.SINGLE_LAYER_INDEX){
      logger.debug("We are currently using a single-layered index and the following code relies on this!")
    }
    val indexSize = index.numLeafNodes
    var processedNodes = 0
    val computedMatches = mutable.HashSet[(TemporalDatabaseTableTrait[Int],TemporalDatabaseTableTrait[Int])]()
    var nMatchesComputed = 0
    logger.debug(s"starting to iterate through ${indexSize} index leaf nodes")
    it.foreach{case g => {
      val potentialTupleMatches = g.tuplesInNode
      val groupsWithTupleIndcies = potentialTupleMatches.groupMap(t => t.table)(t => t.rowIndex).toIndexedSeq
      if(groupsWithTupleIndcies.size>1) {
        logger.debug(s"Processing group ${g.valuesAtTimestamps} with ${groupsWithTupleIndcies.size} tables (head:${groupsWithTupleIndcies.take(5).map(_._1)}) with " +
          s"Top tuple counts: ${groupsWithTupleIndcies.sortBy(-_._2.size).take(5).map{case (t,tuples) => (t.getID,tuples.size)}}")
        println()
      }
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
      processedNodes +=1
      if(processedNodes % 1000==0){
        logger.debug(s"FInished $processedNodes out of $indexSize (${100*processedNodes/indexSize.toDouble}%)")
      }
    }}
    logger.debug(s"Finished Index-Based initial matching, resulting in ${nMatchesComputed} checked matches, of which ${curMatches.map(_._2.size).sum} have a score > 0")
    logger.debug(s"Naive pairwise approach would have had to compute ${gaussSUm(unmatchedAssociations.size)}")
    logger.debug("Begin executing Wildcard matches")
    val wildcards = index.wildCardBucket
    wildcards.foreach(wc => {
      //calculate matches to all other association tables:
      unmatchedAssociations
        .withFilter(a=> a !=wc)
        .foreach(a => {
          ???
          //(first,second) = getCorrectlyOrderedPair()
      })
    })
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
