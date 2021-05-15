package de.hpi.tfm.compatibility.graph.fact.bipartite

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.{FactMatchCreator, TupleReference}
import de.hpi.tfm.compatibility.index.BipartiteTupleIndex
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.util.RuntimeMeasurementUtil.executionTimeInSeconds

import java.time.LocalDate


class BipartiteFactMatchCreator[A](tuplesLeft: IndexedSeq[TupleReference[A]],
                                   tuplesRight: IndexedSeq[TupleReference[A]],
                                   graphConfig: GraphConfig,
                                   filterByCommonWildcardIgnoreChangeTransition:Boolean=true,
                                   var tupleToNonWcTransitions:Option[Map[TupleReference[A], Set[ValueTransition[A]]]]=None,
                                   logProgress:Boolean=false,
                                   logRuntimes:Boolean=false) extends FactMatchCreator[A] with StrictLogging{

  if(filterByCommonWildcardIgnoreChangeTransition) {
    if(!tupleToNonWcTransitions.isDefined){
      tupleToNonWcTransitions = Some((tuplesLeft++tuplesRight)
        .map(t => (t,t.getDataTuple.head
          .valueTransitions(false,true))
        ).toMap)
    }
    assert(tupleToNonWcTransitions.isDefined)
  }

  val detailedLogging:Boolean = false
  var totalIndexTime:Double = 0.0
  var totalMatchExecutionTime = 0.0
  var lastIndexTimeReport = 0.0
  var lastMatchTimeReport = 0.0
  var skippedMatchesDueToFilter = 0
  val programStartInMs = System.currentTimeMillis()
  var computedMatches = 0
  var totalNumTopLvlNodes = -1
  var numProcessedTopLvlNodes = 0
  init()

  def init() = {
    val (index,indexTime) = executionTimeInSeconds(new BipartiteTupleIndex[A](tuplesLeft,tuplesRight,IndexedSeq(),IndexedSeq(),true))
    totalNumTopLvlNodes = if(index.indexFailed) 0 else index.getBipartiteTupleGroupIterator().size
    totalIndexTime += indexTime
    if(detailedLogging){
      val topLevelPairwiseComputations = index.getBipartiteTupleGroupIterator().toIndexedSeq.map(_.totalComputationsIfPairwise)
      logger.debug(s"Constructed Top-Level Index with ${topLevelPairwiseComputations.size} top-level nodes, total possible match computations: ${topLevelPairwiseComputations.map(_.toLong).sum}")
    }
    buildGraph(tuplesLeft,tuplesRight,index,0)
    reportRunTimes()
  }

  def productTooBig(size: Int, size1: Int): Boolean = {
    size*size1>50
  }

  def logProgressNow: Boolean = logProgress && numProcessedTopLvlNodes % (totalNumTopLvlNodes / 1000)==0

  def buildGraph(originalInputLeft:IndexedSeq[TupleReference[A]],
                 originalInputRight:IndexedSeq[TupleReference[A]],
                 index: BipartiteTupleIndex[A],
                 recurseDepth:Int):Unit = {
    if(logProgress && recurseDepth==0)
      logger.debug(s"Iterating through $totalNumTopLvlNodes nodes")
    if(!index.indexFailed){
      val allTuplesLeft = scala.collection.mutable.ArrayBuffer[TupleReference[A]]()
      val allTuplesRight = scala.collection.mutable.ArrayBuffer[TupleReference[A]]()
      index.getBipartiteTupleGroupIterator().foreach{case g => {
        val tuplesLeft = g.tuplesLeft
        val tuplesRight = g.tuplesRight
        val wildcardTuplesLeft = g.wildcardTuplesLeft
        val wildCardTuplesRight = g.wildcardTuplesRight
        assert(g.tuplesLeft.toSet.intersect(wildcardTuplesLeft.toSet).isEmpty && g.tuplesRight.toSet.intersect(wildCardTuplesRight.toSet).isEmpty)
        buildGraphRecursively(g.chosenTimestamps.toIndexedSeq,g.valuesAtTimestamps, tuplesLeft, tuplesRight,recurseDepth+1)
        //TODO: process Wildcards to others:
        allTuplesLeft ++= tuplesLeft
        allTuplesRight ++= tuplesRight
        if(recurseDepth==0)
          numProcessedTopLvlNodes+=1
        if(recurseDepth==0 && logProgressNow)
          logger.debug(s"Finished $numProcessedTopLvlNodes out of $totalNumTopLvlNodes top level nodes(${100*numProcessedTopLvlNodes / totalNumTopLvlNodes.toDouble}%) ")
        //Wildcards to Wildcards:
      }}
      buildGraphRecursively(index.parentTimestamps ++Seq(index.splitT),index.parentKeyValues ++Seq(index.wildcardValues.head),index.wildcardsLeft,index.wildcardsRight,recurseDepth+1)
      buildGraphRecursively(index.parentTimestamps ++Seq(index.splitT),index.parentKeyValues ++Seq(index.wildcardValues.head),index.wildcardsLeft,allTuplesRight.toIndexedSeq,recurseDepth+1)
      buildGraphRecursively(index.parentTimestamps ++Seq(index.splitT),index.parentKeyValues ++Seq(index.wildcardValues.head),allTuplesLeft.toIndexedSeq,index.wildcardsRight,recurseDepth+1)
    } else {
      val (_,time) = executionTimeInSeconds(doPairwiseMatching(originalInputLeft,originalInputRight))
      totalMatchExecutionTime += time
    }
  }

  def reportRunTimes() = {
    if(logRuntimes){
      val timeNow = System.currentTimeMillis()
      logger.debug("----------------------------------------------------------------------------------------------")
      logger.debug(s"Total Index Time: ${totalIndexTime}s")
      logger.debug(s"Total Match Execution Time: ${totalMatchExecutionTime}s")
      val timePassedInSeconds = (timeNow - programStartInMs) / 1000.0
      logger.debug(s"Total Time since constructor: ${timePassedInSeconds}s")
      logger.debug(s"Unaccounted Time: ${timePassedInSeconds - totalMatchExecutionTime - totalIndexTime}s")
      logger.debug(s"Computed Matches: $computedMatches (${computedMatches / timePassedInSeconds} matches per second)")
      logger.debug(s"Filtered Matches: $skippedMatchesDueToFilter (${100*skippedMatchesDueToFilter / (computedMatches + skippedMatchesDueToFilter).toDouble}%)")
      logger.debug("----------------------------------------------------------------------------------------------")
      lastMatchTimeReport = totalMatchExecutionTime
      lastIndexTimeReport = totalIndexTime
    }
  }

  def indexTimeReportDue: Boolean = totalIndexTime - lastIndexTimeReport > 10
  def matchTimeReportDue: Boolean = totalMatchExecutionTime - lastMatchTimeReport > 10

  private def buildGraphRecursively(parentTimestamps:IndexedSeq[LocalDate],
                                    parentValues:IndexedSeq[A],
                                    tuplesLeft: IndexedSeq[TupleReference[A]],
                                    tuplesRight: IndexedSeq[TupleReference[A]],
                                    newRecurseDepth:Int) = {
    if (productTooBig(tuplesLeft.size, tuplesRight.size)) {
      //further index this: new Index
      val (newIndexForSubNode,newIndexTime) = executionTimeInSeconds(new BipartiteTupleIndex[A](tuplesLeft, tuplesRight, parentTimestamps, parentValues, true))
      totalIndexTime += newIndexTime
      if(indexTimeReportDue && detailedLogging){
        reportRunTimes()
      }
      buildGraph(tuplesLeft, tuplesRight, newIndexForSubNode,newRecurseDepth)
    } else {
      val (_,time) = executionTimeInSeconds(doPairwiseMatching(tuplesLeft, tuplesRight))
      totalMatchExecutionTime += time
      if(matchTimeReportDue && detailedLogging){
        reportRunTimes()
      }
    }
  }

  private def doPairwiseMatching(tuplesLeft: IndexedSeq[TupleReference[A]], tuplesRight:IndexedSeq[TupleReference[A]]) = {
    //we construct a graph as an adjacency list:
    //pairwise matching to find out the edge-weights:
    if(tuplesLeft.size>0 && tuplesRight.size>0) {
      for (i <- 0 until tuplesLeft.size) {
        for (j <- 0 until tuplesRight.size) {
          val ref1 = tuplesLeft(i)
          val ref2 = tuplesRight(j)
          if(!filterByCommonWildcardIgnoreChangeTransition || tupleToNonWcTransitions.get(ref1).exists(t => tupleToNonWcTransitions.get(ref2).contains(t))){
            val edge = getTupleMatchOption(ref1, ref2)
            computedMatches +=1
            if (edge.isDefined) {
              facts.add(edge.get)
            }
          } else {
            skippedMatchesDueToFilter +=1
          }
        }
      }
    }
  }

  override def getGraphConfig: GraphConfig = graphConfig
}
