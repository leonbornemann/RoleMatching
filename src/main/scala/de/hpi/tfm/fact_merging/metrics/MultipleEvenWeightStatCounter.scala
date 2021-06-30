package de.hpi.tfm.fact_merging.metrics

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{ChangePoint, CommonPointOfInterestIterator, ValueTransition}
import de.hpi.tfm.evaluation.data.SlimGraphWithoutWeight
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScoreComputer
import de.hpi.tfm.fact_merging.optimization.{EventCountsWithoutWeights, NewEdgeStatRow, SlimGraphSet}
import de.hpi.tfm.io.IOService
import de.hpi.tfm.util.LogUtil
import de.uni_potsdam.hpi.utils.LoggingUtils

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable

class MultipleEvenWeightStatCounter(dsName:String,
                                    graph:SlimGraphWithoutWeight,
                                    tfIDF:Map[LocalDate, TFIDFMapStorage],
                                    TIMESTAMP_GRANULARITY_IN_DAYS:Int,
                                    statFile:File,
                                    graphSetFile:File,
                                    nonInformativeValuesIsStrict:Boolean =false) extends StrictLogging{

  assert(graph.allEndTimes == tfIDF.keySet)

  //for the sake of simplicity for now: change this later
  val tfIDFTableAsMap = tfIDF.map(t => (t._1,t._2.asMap))

  val isWildcard:(Any => Boolean) = graph.verticesOrdered.head.factLineage.toFactLineage.isWildcard
  val transitionSets = graph.verticesOrdered.zipWithIndex
    .map(il => (il._2,il._1.factLineage.toFactLineage.valueTransitions(true,false)))
    .toMap
  val nonInformativeValues = GLOBAL_CONFIG.nonInformativeValues

  def getEventCounts(cp: ChangePoint[Any],vertexIdFirst:Int,vertexIdSecond:Int,trainTimeEnd:LocalDate) = {
    assert(!cp.prevPointInTime.isAfter(trainTimeEnd))
    val totalCounts = new MultipleEventWeightScoreOccurrenceStats(null,null)
    val countPrev = MultipleEventWeightScoreComputer.getCountPrev(cp,TIMESTAMP_GRANULARITY_IN_DAYS,Some(trainTimeEnd)).toInt
    if(countPrev>0){
      val countPrevTransiton = MultipleEventWeightScoreComputer.getCountForSameValueTransition(cp.prevValueA,cp.prevValueB,countPrev,isWildcard,
        transitionSets(vertexIdFirst),transitionSets(vertexIdSecond),nonInformativeValues,nonInformativeValuesIsStrict,Some(tfIDFTableAsMap(trainTimeEnd)))
      if(countPrevTransiton.isDefined) totalCounts.addAll(countPrevTransiton.get)
    }
    if(!cp.pointInTime.isAfter(trainTimeEnd)){
      val countCurrent = MultipleEventWeightScoreComputer.getCountForTransition(cp,isWildcard,
        transitionSets(vertexIdFirst),transitionSets(vertexIdSecond),nonInformativeValues,nonInformativeValuesIsStrict,Some(tfIDFTableAsMap(trainTimeEnd)))
      if(countCurrent.isDefined) totalCounts.addAll(countCurrent.get)
    }
    totalCounts
  }

  def aggregateEventCounts() = {
    val totalCounts = scala.collection.mutable.HashMap[LocalDate, MultipleEventWeightScoreOccurrenceStats]()
    val trainTimeEndsWithIndex = graph.trainTimeEnds.zipWithIndex
    val latestTime = graph.trainTimeEnds.max
    var processedEdges = 0
    val statPr = new PrintWriter(statFile)
    val adjacencyList = collection.mutable.HashMap[Int, collection.mutable.HashMap[Int, Seq[EventCountsWithoutWeights]]]()
    graph.generalEdgeIterator.foreach { case (firstNode, secondNode, e, isEdgeInGraph) => {
      val totalCountsThisEdge = scala.collection.mutable.HashMap[LocalDate, MultipleEventWeightScoreOccurrenceStats]()
      val commonPointOfInterestIterator = new CommonPointOfInterestIterator[Any](e.v1.factLineage.toFactLineage, e.v2.factLineage.toFactLineage)
      commonPointOfInterestIterator
        .withFilter(cp => !cp.prevPointInTime.isAfter(latestTime))
        .foreach(cp => {
          //val eventCounts = getEventCounts(cp, firstNode, secondNode)
          if (!cp.prevPointInTime.isAfter(graph.smallestTrainTimeEnd)) {
            val eventCountsSmallestTimeEnd = getEventCounts(cp, firstNode, secondNode,graph.smallestTrainTimeEnd)
            addToTotal(totalCounts, eventCountsSmallestTimeEnd,graph.smallestTrainTimeEnd)
            addToTotal(totalCountsThisEdge, eventCountsSmallestTimeEnd,graph.smallestTrainTimeEnd)
          }
          trainTimeEndsWithIndex.foreach { case (ld, i) => {
            if (isEdgeInGraph(i) && !cp.prevPointInTime.isAfter(ld)) {
              val eventCounts = getEventCounts(cp, firstNode, secondNode,ld)
              addToTotal(totalCounts, eventCounts,ld)
              addToTotal(totalCountsThisEdge, eventCounts,ld)
            }
          }
          }
        })
      //add to stats:
      val statRows = totalCountsThisEdge.values.toIndexedSeq.map(v => NewEdgeStatRow(e,v))
      if(processedEdges==0)
        statPr.println(statRows.head.getSchema.mkString(","))
      statRows.sortBy(_.scoreStats.trainTimeEnd.toEpochDay).foreach(sr => statPr.println(sr.getStatRow.mkString(",")))
      //add to SlimGraphSet:
      assert(firstNode<secondNode)
      val countsSorted = totalCountsThisEdge.toIndexedSeq.sortBy(_._1.toEpochDay)
        .map(t => EventCountsWithoutWeights.from(t._2))
      val map = adjacencyList.getOrElseUpdate(firstNode,collection.mutable.HashMap[Int, Seq[EventCountsWithoutWeights]]())
      map.put(secondNode,countsSorted)
      processedEdges+=1
      val toLog = LogUtil.buildLogProgressStrings(processedEdges,100000,None,"edges")
      if(toLog.isDefined)
        logger.debug(toLog.get)
    }}
    val trainTimeEndsSorted = graph.allEndTimes.toIndexedSeq.sortBy(_.toEpochDay)
    val graphSet = SlimGraphSet(graph.verticesOrdered.map(_.id),trainTimeEndsSorted,adjacencyList)
    graphSet.toJsonFile(graphSetFile)
    statPr.close()
    totalCounts
  }

  private def addToTotal(totalCounts: mutable.HashMap[LocalDate, MultipleEventWeightScoreOccurrenceStats],
                         eventCounts: MultipleEventWeightScoreOccurrenceStats,
                         date:LocalDate) = {
    totalCounts.getOrElseUpdate(date, new MultipleEventWeightScoreOccurrenceStats(dsName, date))
      .addAll(eventCounts)
  }
}
