package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.{GLOBAL_CONFIG, InitialMatchinStrategy}
import de.hpi.dataset_versioning.db_synthesis.baseline.config.InitialMatchinStrategy.InitialMatchinStrategy
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.dataset_versioning.db_synthesis.baseline.index.{MostDistinctTimestampIndexBuilder, TupleGroup, TupleSetIndex}

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class AssociationClusterer(unmatchedAssociations: mutable.HashSet[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch],
                           heuristicMatchCalulator:DataBasedMatchCalculator) extends StrictLogging {
  def removeMatch(curMatch: TableUnionMatch[Int]) = ???

  def updateGraphAfterMatchExecution(curMatch: TableUnionMatch[Int], unionedTableSketch: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = ???


  val adjacencyList = unmatchedAssociations
    .map(a => (a.asInstanceOf[TemporalDatabaseTableTrait[Int]],mutable.HashMap[TemporalDatabaseTableTrait[Int],TableUnionMatch[Int]]()))
    .toMap
  val matchesWithZeroScore = mutable.HashSet[Set[TemporalDatabaseTableTrait[Int]]]()
  var nMatchesComputed = 0
  var processedNodes = 0
  var topLvlIndexSize = -1
  initMatchGraph()
  //create sorted list
  val sortedMatches = adjacencyList
    .flatMap{case (t,adjacent) => adjacent.map(_._2)}
    .toSet
    .toIndexedSeq
    .sortBy( (m:TableUnionMatch[Int]) => m.score)(Ordering[Float].reverse)


  def matchWasAlreadyCalculated(firstMatchPartner: TemporalDatabaseTableTrait[Int], secondMatchPartner: TemporalDatabaseTableTrait[Int]) = {
    val existsWithScoreGreater0 = adjacencyList(firstMatchPartner).contains(secondMatchPartner)
    if(existsWithScoreGreater0) assert(adjacencyList(secondMatchPartner).contains(firstMatchPartner))
    existsWithScoreGreater0 || matchesWithZeroScore.contains(Set(firstMatchPartner,secondMatchPartner))
  }

  def executePairwiseMatching(groupsWithTupleIndices: collection.IndexedSeq[TemporalDatabaseTableTrait[Int]]) = {
    for (i <- 0 until groupsWithTupleIndices.size) {
      for (j <- (i + 1) until groupsWithTupleIndices.size) {
        val firstMatchPartner = groupsWithTupleIndices(i)
        val secondMatchPartner = groupsWithTupleIndices(j)
        calculateAndMatchIfNotPresent(firstMatchPartner, secondMatchPartner)
      }
    }
  }

  private def calculateAndMatchIfNotPresent(firstMatchPartner: TemporalDatabaseTableTrait[Int], secondMatchPartner: TemporalDatabaseTableTrait[Int]) = {
    if (!matchWasAlreadyCalculated(firstMatchPartner, secondMatchPartner)) {
      val curMatch = heuristicMatchCalulator.calculateMatch(firstMatchPartner, secondMatchPartner)
      nMatchesComputed += 1
      if (curMatch.score != 0) {
        adjacencyList(curMatch.firstMatchPartner).put(curMatch.secondMatchPartner, curMatch)
        adjacencyList(curMatch.secondMatchPartner).put(curMatch.firstMatchPartner, curMatch)
      } else{
        //register that we should not try this again, even though we had
        matchesWithZeroScore.add(Set(firstMatchPartner,secondMatchPartner))
      }
    }
  }

  def gaussSum(n: Int) = n*(n+1) / 2

  def executeMatchesInIterator(it: Iterator[TupleGroup[Int]],
                               wildCardNodes: Iterable[TupleReference[Int]],
                               recurseDepth:Int):Unit = {
    val isTopLvlCall = recurseDepth==0
    it.foreach{case g => {
      val potentialTupleMatches = g.tuplesInNode
      val groupsWithTupleIndices = potentialTupleMatches.groupMap(t => t.table)(t => t.rowIndex).toIndexedSeq
      if(groupsWithTupleIndices.size>1) {
        logger.debug(s"Processing group ${g.valuesAtTimestamps} with ${groupsWithTupleIndices.size} tables (head:${groupsWithTupleIndices.take(5).map(_._1)}) with " +
          s"Top tuple counts: ${groupsWithTupleIndices.sortBy(-_._2.size).take(5).map{case (t,tuples) => (t.getID,tuples.size)}} [Recurse Depth:$recurseDepth]")
        println()
      }
      if(gaussSum(groupsWithTupleIndices.size)>100){
        assert(g.chosenTimestamps.size==g.valuesAtTimestamps.size)
        val newIndex = new TupleSetIndex[Int]((potentialTupleMatches).toIndexedSeq,
          g.chosenTimestamps.toIndexedSeq,
          g.valuesAtTimestamps,
          potentialTupleMatches.head.table.wildcardValues.toSet)
        if(newIndex.indexBuildWasSuccessfull) {
          logger.debug(s"Starting recursive call because size ${groupsWithTupleIndices.size} is too large [Recurse Depth:$recurseDepth]")
          executeMatchesInIterator(newIndex.tupleGroupIterator(true),newIndex.getWildcardBucket,recurseDepth+1)
        } else {
          logger.debug(s"Executing pairwise matching with ${groupsWithTupleIndices.size} because we can't refine the index anymore [Recurse Depth:$recurseDepth]")
          executePairwiseMatching(groupsWithTupleIndices.map(_._1))
        }
      } else {
        executePairwiseMatching(groupsWithTupleIndices.map(_._1))
      }
      processedNodes +=1
      if(isTopLvlCall && processedNodes % 1000==0){
        logger.debug(s"FInished $processedNodes top lvl nodes out of $topLvlIndexSize (${100*processedNodes/topLvlIndexSize.toDouble}%)")
      }
    }}
    if(isTopLvlCall) {
      logger.debug(s"Finished Index-Based initial matching, resulting in ${nMatchesComputed} checked matches, of which ${adjacencyList.map(_._2.size).sum / 2} have a score > 0")
      logger.debug("Begin executing Wildcard matches FOR TOP-LVL")
    }
    val wildcardTables = wildCardNodes
      .map(_.table)
      .toIndexedSeq
    wildcardTables.foreach(wc => {
      //calculate matches to all other association tables:
      unmatchedAssociations
        .withFilter(a=> a !=wc)
        .foreach(a => {
          calculateAndMatchIfNotPresent(wc,a)
        })
    })
    //calculate matches to all other wildcards
    executePairwiseMatching(wildcardTables)
  }

  def initMatchGraph() = {
    logger.debug("Starting Index-Based initial match computation")
    val indexBuilder = new MostDistinctTimestampIndexBuilder[Int](unmatchedAssociations.map(_.asInstanceOf[TemporalDatabaseTableTrait[Int]]))
    val index = indexBuilder.buildTableIndexOnNonKeyColumns()
    val it = index.tupleGroupIterator
    //index.serializeDetailedStatistics()
    if(GLOBAL_CONFIG.SINGLE_LAYER_INDEX){
      logger.debug("We are currently using a single-layered index and the following code relies on this!")
    }
    topLvlIndexSize = index.numLeafNodes
    logger.debug(s"starting to iterate through ${topLvlIndexSize} index leaf nodes")
    executeMatchesInIterator(it,index.wildCardBucket,0)
  }

}
