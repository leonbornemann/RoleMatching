package de.hpi.tfm.compatibility

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.association.{AssociationGraphEdgeCandidate, AssociationMatch}
import de.hpi.tfm.compatibility.graph.fact.{FactMergeabilityGraphEdge, TupleReference}
import de.hpi.tfm.compatibility.index.{BipartiteTupleIndex, IterableTupleIndex, MostDistinctTimestampIndexBuilder, TupleGroup, TupleSetIndex}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.TemporalDatabaseTableTrait
import de.hpi.tfm.data.tfmp_input.table.sketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.DBSynthesis_IOService
import de.hpi.tfm.util.RuntimeMeasurementUtil.executionTimeInSeconds

import java.io.{FileWriter, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable

class CompatiblityGraphCreator(unmatchedAssociations: collection.Set[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch],
                               datasetName:String,
                               recurseLogDepth:Int = 0)  extends StrictLogging{

  private val numFacts: Long = unmatchedAssociations.map(_.nrows.toLong).sum
  logger.debug(s"Starting compatiblity graph creation with ${unmatchedAssociations.size} associations containing $numFacts facts --> ${gaussSum(numFacts-1)} matches possible")
  val edgeCandidateFileWriter = new PrintWriter(FactMergeabilityGraphEdge.getEdgeCandidateJsonPerLineFile(datasetName))
  var indexTimeInSeconds:Double = 0.0
  var bipartiteIndexTimeInSeconds:Double = 0.0
  var matchTimeInSeconds:Double = 0.0
  var bipartiteMatchTimeInSeconds:Double = 0.0
  var pairwiseInnerLoopExecutions = 0

  var nMatchesComputed = 0
  var processedTopLvlNodes = 0
  var processedTopLvlBipartiteNodes = 0
  var topLvlIndexSize = -1
  var topLvlBipartiteIndexSize = -1
  initMatchGraph()

  def serializeCandidate(f1: TupleReference[Int], f2: TupleReference[Int],isBipartite:Boolean) = {
    val (mergeIsValid,execTime) = executionTimeInSeconds(f1.getDataTuple.head.tryMergeWithConsistent(f2.getDataTuple.head).isDefined)
    if(!isBipartite)
      matchTimeInSeconds += execTime
    else
      bipartiteMatchTimeInSeconds +=execTime
    if(mergeIsValid){
      FactMergeabilityGraphEdge(f1.toIDBasedTupleReference,f2.toIDBasedTupleReference, -1)
        .appendToWriter(edgeCandidateFileWriter,false,true)
    }
  }

  def executePairwiseMatching(facts: collection.IndexedSeq[TupleReference[Int]],
                              filterByCommonTransitionOverlap:Boolean = false) = {
    for (i <- 0 until facts.size) {
      for (j <- (i + 1) until facts.size) {
        pairwiseInnerLoopExecutions += 1
        serializeCandidate(facts(i), facts(j),false)
      }
    }
  }

  def gaussSum(n: Long) = n*(n+1) / 2

  def logRecursionWhitespacePrefix(depth:Int) = {
    var curRepeat = depth
    val sb = new StringBuilder()
    while(curRepeat>0) {
      sb.append("  ")
      curRepeat -=1
    }
    sb.toString()
  }

  def maybeLog(str: String, recurseDepth: Int) = {
    if(recurseDepth<=recurseLogDepth)
      logger.debug(str)
  }

  def checkAllBipartiteMatches(left: IndexedSeq[TupleReference[Int]], right: IndexedSeq[TupleReference[Int]]) = {
    left.foreach(a => {
      //calculate matches to all other association tables:
      right.foreach(b => {
        pairwiseInnerLoopExecutions += 1
        serializeCandidate(a, b,true)
      })
    })
  }

  def bipartiteMatchGraphConstruction(left:IndexedSeq[TupleReference[Int]],
                                      right:IndexedSeq[TupleReference[Int]],
                                      parentTimestamps:IndexedSeq[LocalDate],
                                      parentKeyValues:IndexedSeq[Int],
                                      recurseDepth:Int):Unit = {
    if(left.size*right.size<50){
      checkAllBipartiteMatches(left,right)
    }
    val isTopLevelCall = recurseDepth == 0
    val (index,indexTime) = executionTimeInSeconds(new BipartiteTupleIndex(left,right,parentTimestamps,parentKeyValues))
    bipartiteIndexTimeInSeconds +=indexTime
    if(index.indexFailed){
      checkAllBipartiteMatches(left,right)
    } else {
      if(isTopLevelCall){
        topLvlBipartiteIndexSize = index.numLeafNodes
      }
      val groupIterator = index.getBipartiteTupleGroupIterator()
      val wildcardsLeft = index.wildcardsLeft
      val wildcardsRight = index.wildcardsRight
      groupIterator.foreach(g => {
        maybeLog(s"${logRecursionWhitespacePrefix(recurseDepth)} Processing Bipartite group ${g.valuesAtTimestamps} with ${g.tuplesLeft.size} facts on left and ${g.tuplesRight.size} facts on right [Recurse Depth:$recurseDepth]",recurseDepth)
        maybeLog(s"Index Time:${f"$indexTimeInSeconds%1.3f"}s, Match time:${f"$matchTimeInSeconds%1.3f"}   Bipartite Index Time:${f"$bipartiteIndexTimeInSeconds%1.3f"}s, Bipartite Match time:${f"$bipartiteMatchTimeInSeconds%1.3f"}",recurseDepth)
        if(g.tuplesLeft.size>0 && wildcardsRight.size>0){
          bipartiteMatchGraphConstruction(g.tuplesLeft,wildcardsRight,g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps,recurseDepth+1)
        }
        if(g.tuplesRight.size>0 && wildcardsLeft.size>0){
          bipartiteMatchGraphConstruction(wildcardsLeft,g.tuplesRight,g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps,recurseDepth+1)
        }
        if(g.tuplesLeft.size>0 && g.tuplesRight.size>0){
          bipartiteMatchGraphConstruction(g.tuplesLeft,g.tuplesRight,g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps,recurseDepth+1)
        }
        if(isTopLevelCall)
          processedTopLvlBipartiteNodes +=1
        if(isTopLevelCall && processedTopLvlBipartiteNodes % 1000==0){
          logger.debug(s"FInished $processedTopLvlBipartiteNodes top lvl bipartite nodes out of $topLvlBipartiteIndexSize (${100*processedTopLvlBipartiteNodes/topLvlBipartiteIndexSize.toDouble}%)")
        }
      })
      if(wildcardsLeft.size>0 && wildcardsRight.size>0) {
        bipartiteMatchGraphConstruction(wildcardsLeft,
          wildcardsRight,
          index.getBipartiteTupleGroupIterator().next().chosenTimestamps.toIndexedSeq,
          index.parentKeyValues ++ IndexedSeq(index.wildcardValues.head),recurseDepth+1)
      }
    }
  }

  def runIndexBasedMatching(oldIndex:IterableTupleIndex[Int],
                            recurseDepth:Int):Unit = {
    //aggregate to single WC-Bucket:
    val wcTuples = oldIndex.wildcardBuckets.flatMap(_.wildcardTuples)
    if(!wcTuples.isEmpty){
      //TODO: build double-sided index to determine matches
      bipartiteCompatiblityGraphCreation(oldIndex, recurseDepth, wcTuples)
      //WC-TO-WC matches:
      val (newIndex,time) = executionTimeInSeconds(new TupleSetIndex[Int]((wcTuples),
        oldIndex.wildcardBuckets.head.chosenTimestamps.toIndexedSeq,
        oldIndex.wildcardBuckets.head.valuesAtTimestamps,
        wcTuples.head.table.wildcardValues.toSet,
        true))
      indexTimeInSeconds += time
      if(newIndex.indexBuildWasSuccessfull)
        runIndexBasedMatching(newIndex,recurseDepth+1)
      else
        executePairwiseMatching(wcTuples,false)
    }
    compatibilityGraphConstruction(oldIndex,recurseDepth)
  }

  private def bipartiteCompatiblityGraphCreation(oldIndex: IterableTupleIndex[Int], recurseDepth: Int, wcTuples: IndexedSeq[TupleReference[Int]]) = {
    val isTopLvlCall = recurseDepth==0
    if (isTopLvlCall) {
      logger.debug(s"Finished Index-Based initial matching, resulting in ${nMatchesComputed} found edges")
      logger.debug("Begin executing Wildcard matches FOR TOP-LVL")
      logger.debug(s"Found ${wcTuples.size} wildcard facts")
    }
    val nonWildCards = oldIndex
      .tupleGroupIterator(true)
      .toIndexedSeq
      .flatMap(_.tuplesInNode)
    if (nonWildCards.size > 0 && wcTuples.size > 0) {
      bipartiteMatchGraphConstruction(wcTuples,
        nonWildCards,
        oldIndex.tupleGroupIterator(true).next().chosenTimestamps.toIndexedSeq,
        oldIndex.getParentKeyValues ++ IndexedSeq(wcTuples.head.table.wildcardValues.head),
        recurseDepth)
    }
    if (isTopLvlCall) {
      logger.debug(s"Finished Bipartite matching")
    }
  }

  private def compatibilityGraphConstruction(oldIndex: IterableTupleIndex[Int],recurseDepth:Int) = {
    val isTopLvlCall = recurseDepth==0
    val it = oldIndex.tupleGroupIterator(true)
    it.foreach { case g => {
      val potentialTupleMatches = g.tuplesInNode
      if (potentialTupleMatches.size > 1) {
        maybeLog(s"${logRecursionWhitespacePrefix(recurseDepth)}Processing group ${g.valuesAtTimestamps} with ${potentialTupleMatches.size} facts [Recurse Depth:$recurseDepth]", recurseDepth)
        maybeLog(s"Index Time:${f"$indexTimeInSeconds%1.3f"}s, Match time:${f"$matchTimeInSeconds%1.3f"}", recurseDepth)
      }
      if (potentialTupleMatches.size > 2) {
        assert(g.chosenTimestamps.size == g.valuesAtTimestamps.size)
        val (newIndex, time) = executionTimeInSeconds(new TupleSetIndex[Int]((potentialTupleMatches).toIndexedSeq,
          g.chosenTimestamps.toIndexedSeq,
          g.valuesAtTimestamps,
          potentialTupleMatches.head.table.wildcardValues.toSet,
          true))
        indexTimeInSeconds += time
        if (newIndex.indexBuildWasSuccessfull) {
          //maybeLog(s"${logRecursionWhitespacePrefix(recurseDepth)}Starting recursive call because size ${groupsWithTupleIndices.size} is too large [Recurse Depth:$recurseDepth]",recurseDepth)
          runIndexBasedMatching(newIndex, recurseDepth + 1)
        } else {
          maybeLog(s"${logRecursionWhitespacePrefix(recurseDepth)}Executing pairwise matching with ${potentialTupleMatches.size} because we can't refine the index anymore [Recurse Depth:$recurseDepth]", recurseDepth)
          executePairwiseMatching(potentialTupleMatches.toIndexedSeq, false)
        }
      } else {
        executePairwiseMatching(potentialTupleMatches.toIndexedSeq, false)
      }
      if (isTopLvlCall)
        processedTopLvlNodes += 1
      if (isTopLvlCall && processedTopLvlNodes % 1000 == 0) {
        logger.debug(s"FInished $processedTopLvlNodes top lvl nodes out of $topLvlIndexSize (${100 * processedTopLvlNodes / topLvlIndexSize.toDouble}%)")
      }
    }
    }
  }

  def initMatchGraph() = {
    logger.debug("Starting Index-Based compatibility graph computation")
    val indexBuilder = new MostDistinctTimestampIndexBuilder[Int](unmatchedAssociations.map(_.asInstanceOf[TemporalDatabaseTableTrait[Int]]))
    val (index,time) = executionTimeInSeconds(indexBuilder.buildTableIndexOnNonKeyColumns())
    indexTimeInSeconds +=time
    topLvlIndexSize = index.numLeafNodes
    logger.debug(s"starting to iterate through ${topLvlIndexSize} index leaf nodes")
    runIndexBasedMatching(index,0)
    edgeCandidateFileWriter.close()
    logger.debug(s"Finished with $pairwiseInnerLoopExecutions num inner pairwise matching loop executions")
  }

}
