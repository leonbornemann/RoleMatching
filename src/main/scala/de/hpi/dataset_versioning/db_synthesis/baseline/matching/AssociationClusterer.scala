package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.{GLOBAL_CONFIG, InitialMatchinStrategy}
import de.hpi.dataset_versioning.db_synthesis.baseline.config.InitialMatchinStrategy.InitialMatchinStrategy
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.index.{MostDistinctTimestampIndexBuilder, TupleGroup, TupleSetIndex}
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.IndexProcessingMode.IndexProcessingMode
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import java.io.PrintWriter
import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class AssociationClusterer(unmatchedAssociations: mutable.HashSet[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch],
                           heuristicMatchCalulator:DataBasedMatchCalculator,
                           indexProcessingMode:IndexProcessingMode.Value = IndexProcessingMode.SERIALIZE_EDGE_CANDIDATE) extends StrictLogging {
  def removeMatch(curMatch: TableUnionMatch[Int]) = ???

  def updateGraphAfterMatchExecution(curMatch: TableUnionMatch[Int], unionedTableSketch: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = ???

  val recurseLogDepth = 1
  logger.debug(s"Starting association clustering with ${unmatchedAssociations.size} associations --> ${gaussSum(unmatchedAssociations.size-1)} matches possible")
  val associationGraphEdgeWriter = new PrintWriter(DBSynthesis_IOService.getAssociationGraphEdgeFile)
  val matchTimeWriter = new PrintWriter(DBSynthesis_IOService.WORKING_DIR + "matchTimes.csv")
  matchTimeWriter.println(s"tableA,tableB,nrowsA,nrowsB,time[s]")
  var indexTimeInSeconds:Double = 0.0
  var matchTimeInSeconds:Double = 0.0
  var matchSkips = 0
  var uncomputedEdgeCandidates = if(indexProcessingMode==IndexProcessingMode.SERIALIZE_EDGE_CANDIDATE) Some(new mutable.HashSet[Set[DecomposedTemporalTableIdentifier]]()) else None

  var tableGraphEdges = mutable.HashSet[AssociationGraphEdge]()

  val adjacencyList = unmatchedAssociations
    .map(a => (a.asInstanceOf[TemporalDatabaseTableTrait[Int]],mutable.HashMap[TemporalDatabaseTableTrait[Int],TableUnionMatch[Int]]()))
    .toMap
  val matchesWithZeroScore = mutable.HashSet[Set[TemporalDatabaseTableTrait[Int]]]()
  var nMatchesComputed = 0
  var processedNodes = 0
  var topLvlIndexSize = -1
  initMatchGraph()
  associationGraphEdgeWriter.close()
  matchTimeWriter.close()
  //create sorted list
  val sortedMatches = adjacencyList
    .flatMap{case (t,adjacent) => adjacent.map(_._2)}
    .toSet
    .toIndexedSeq
    .sortBy( (m:TableUnionMatch[Int]) => m.evidence)(Ordering[Int].reverse)

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
    if(IndexProcessingMode==IndexProcessingMode.SERIALIZE_EDGE_CANDIDATE){
      uncomputedEdgeCandidates.get.add(Set(firstMatchPartner.getUnionedOriginalTables.head,secondMatchPartner.getUnionedOriginalTables.head))
    } else if (!matchWasAlreadyCalculated(firstMatchPartner, secondMatchPartner)) {
      val (curMatch,time) = executionTimeInSeconds(heuristicMatchCalulator.calculateMatch(firstMatchPartner, secondMatchPartner))
      matchTimeWriter.println(s"${firstMatchPartner.getUnionedOriginalTables.head},${secondMatchPartner.getUnionedOriginalTables.head},${firstMatchPartner.nrows},${secondMatchPartner.nrows},$time")
      matchTimeWriter.flush()
      matchTimeInSeconds +=time
      nMatchesComputed += 1
      if (curMatch.evidence != 0) {
        adjacencyList(curMatch.firstMatchPartner).put(curMatch.secondMatchPartner, curMatch)
        adjacencyList(curMatch.secondMatchPartner).put(curMatch.firstMatchPartner, curMatch)
        val newAssociationGraphEdge = AssociationGraphEdge(curMatch.firstMatchPartner.getUnionedOriginalTables.head, curMatch.secondMatchPartner.getUnionedOriginalTables.head, curMatch.evidence, curMatch.changeBenefit._1,curMatch.changeBenefit._2)
        tableGraphEdges.add(newAssociationGraphEdge)
        newAssociationGraphEdge.appendToWriter(associationGraphEdgeWriter,false,true,true)
      } else{
        //register that we should not try this again, even though we had
        matchesWithZeroScore.add(Set(firstMatchPartner,secondMatchPartner))
      }
      if(nMatchesComputed%1000==0){
        logger.debug(s"Completed ${nMatchesComputed} match calculations out of ${gaussSum(unmatchedAssociations.size-1)} potential matches (${100*nMatchesComputed/gaussSum(unmatchedAssociations.size-1).toDouble}%)")
      }
    } else{
      matchSkips +=1
    }
  }

  def executionTimeInSeconds[R](block: => R): (R,Double) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val resultTime = (t1-t0)/1000000000.0
    (result,resultTime)
  }


  def gaussSum(n: Int) = n*(n+1) / 2

  def logRecursionWhitespacePrefix(depth:Int) = {
    var curRepeat = depth
    val sb = new StringBuilder()
    while(curRepeat>0) {
      sb.append("  ")
      curRepeat -=1
    }
    sb.toString()
  }

  def nonZeroScoreMatches = tableGraphEdges.size
  def maybeLog(str: String, recurseDepth: Int) = {
    if(recurseDepth<=recurseLogDepth)
      logger.debug(str)
  }

  def executeMatchesInIterator(it: Iterator[TupleGroup[Int]],
                               wildCardNodes: Iterable[TupleReference[Int]],
                               recurseDepth:Int):Unit = {
    val isTopLvlCall = recurseDepth==0
    it.foreach{case g => {
      val potentialTupleMatches = g.tuplesInNode
      val groupsWithTupleIndices = potentialTupleMatches.groupMap(t => t.table)(t => t.rowIndex).toIndexedSeq
      if(groupsWithTupleIndices.size>1) {
        maybeLog(s"${logRecursionWhitespacePrefix(recurseDepth)}Processing group ${g.valuesAtTimestamps} with ${groupsWithTupleIndices.size} tables [Recurse Depth:$recurseDepth]",recurseDepth)
        maybeLog(s"Index Time:${f"$indexTimeInSeconds%1.3f"}s, Match time:${f"$matchTimeInSeconds%1.3f"}s, 0-score matches: ${matchesWithZeroScore.size}, non-zero score matches: ${nonZeroScoreMatches}, match-Skips:$matchSkips",recurseDepth)
      }
      if(potentialTupleMatches.exists(_.getDataTuple.head.countChanges(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD)._1<=0)){
        logger.debug("Really weird, we found a tuple with zero changes, when we were not supposed to")
      }
      if(groupsWithTupleIndices.size>2){
        assert(g.chosenTimestamps.size==g.valuesAtTimestamps.size)
        val (newIndex,time) = executionTimeInSeconds(new TupleSetIndex[Int]((potentialTupleMatches).toIndexedSeq,
          g.chosenTimestamps.toIndexedSeq,
          g.valuesAtTimestamps,
          potentialTupleMatches.head.table.wildcardValues.toSet,
          true))
        indexTimeInSeconds +=time
        if(newIndex.indexBuildWasSuccessfull) {
          //maybeLog(s"${logRecursionWhitespacePrefix(recurseDepth)}Starting recursive call because size ${groupsWithTupleIndices.size} is too large [Recurse Depth:$recurseDepth]",recurseDepth)
          executeMatchesInIterator(newIndex.tupleGroupIterator(true),newIndex.getWildcardBucket,recurseDepth+1)
        } else {
          maybeLog(s"${logRecursionWhitespacePrefix(recurseDepth)}Executing pairwise matching with ${groupsWithTupleIndices.size} because we can't refine the index anymore [Recurse Depth:$recurseDepth]",recurseDepth)
          executePairwiseMatching(groupsWithTupleIndices.map(_._1))
        }
      } else {
        executePairwiseMatching(groupsWithTupleIndices.map(_._1))
      }
      if(isTopLvlCall)
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
    val (index,time) = executionTimeInSeconds(indexBuilder.buildTableIndexOnNonKeyColumns())
    indexTimeInSeconds +=time
    val it = index.tupleGroupIterator
    //index.serializeDetailedStatistics()
    if(GLOBAL_CONFIG.SINGLE_LAYER_INDEX){
      logger.debug("We are currently using a single-layered index and the following code relies on this!")
    }
    topLvlIndexSize = index.numLeafNodes
    logger.debug(s"starting to iterate through ${topLvlIndexSize} index leaf nodes")
    executeMatchesInIterator(it,index.wildCardBucket,0)
    if(indexProcessingMode==IndexProcessingMode.SERIALIZE_EDGE_CANDIDATE){
      logger.debug("Beginning serialization of edge candidates")
      val pr = new PrintWriter(DBSynthesis_IOService.getAssociationGraphEdgeCandidateFile)
      uncomputedEdgeCandidates.get.foreach(s => {
        val res = s.toSeq
        pr.println(AssociationGraphEdge(res(0),res(1),Integer.MIN_VALUE,Integer.MIN_VALUE,Integer.MIN_VALUE).toJson())
      })
      pr.close()
      logger.debug("Finished serialization of edge candidates - terminating now with AssertionError as there is no need to continue")
      assert(false)
    }
  }

}
