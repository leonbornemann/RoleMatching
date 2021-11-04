package de.hpi.role_matching.cbrm.evidence_based_weighting

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.slim.{MemoryEfficientCompatiblityGraphSet, MemoryEfficientCompatiblityGraphWithoutEdgeWeight}
import de.hpi.role_matching.cbrm.data.{ChangePoint, CommonPointOfInterestIterator}
import de.hpi.role_matching.cbrm.evidence_based_weighting.isf.ISFMapStorage
import de.hpi.role_matching.evaluation.tuning
import de.hpi.util.LogUtil

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable
import scala.util.Random

class EvidenceBasedWeightingEventCounter(dsName:String,
                                         graph:MemoryEfficientCompatiblityGraphWithoutEdgeWeight,
                                         tfIDF:Map[LocalDate, ISFMapStorage],
                                         TIMESTAMP_GRANULARITY_IN_DAYS:Int,
                                         statFile:File,
                                         graphSetFile:File,
                                         nonInformativeValuesIsStrict:Boolean =false) extends StrictLogging{

  assert(graph.allEndTimes == tfIDF.keySet)

  //for the sake of simplicity for now: change this later
  val tfIDFTableAsMap = tfIDF.map(t => (t._1,t._2.asMap))

  val isWildcard:(Any => Boolean) = graph.verticesOrdered.head.roleLineage.toRoleLineage.isWildcard
  val transitionSets = graph.verticesOrdered.zipWithIndex
    .map(il => (il._2,il._1.roleLineage.toRoleLineage.valueTransitions(true,false)))
    .toMap
  val nonInformativeValues = GLOBAL_CONFIG.nonInformativeValues

  def getEventCounts(cp: ChangePoint,vertexIdFirst:Int,vertexIdSecond:Int,trainTimeEnd:LocalDate) = {
    assert(!cp.prevPointInTime.isAfter(trainTimeEnd))
    val totalCounts = new EventOccurrenceStatistics(null,null)
    val countPrev = EvidenceBasedWeightingScoreComputer.getCountPrev(cp,TIMESTAMP_GRANULARITY_IN_DAYS,Some(trainTimeEnd)).toInt
    if(countPrev>0){
      val countPrevTransiton = EvidenceBasedWeightingScoreComputer.getCountForSameValueTransition(cp.prevValueA,cp.prevValueB,countPrev,isWildcard,
        transitionSets(vertexIdFirst),transitionSets(vertexIdSecond),nonInformativeValues,nonInformativeValuesIsStrict,Some(tfIDFTableAsMap(trainTimeEnd)))
      if(countPrevTransiton.isDefined)
        totalCounts.addAll(countPrevTransiton.get)
    }
    if(!cp.pointInTime.isAfter(trainTimeEnd)){
      val countCurrent = EvidenceBasedWeightingScoreComputer.getCountForTransition(cp,isWildcard,
        transitionSets(vertexIdFirst),transitionSets(vertexIdSecond),nonInformativeValues,nonInformativeValuesIsStrict,Some(tfIDFTableAsMap(trainTimeEnd)))
      //assert(countCurrent.isDefined)
      if(countCurrent.isDefined) {
        totalCounts.addAll(countCurrent.get)
      }
      //if this is the last one we have more same value transitions until the end of trainTimeEnd
      if(cp.isLast && countCurrent.isDefined && cp.pointInTime.isBefore(trainTimeEnd)){
        val countAfterInDays = trainTimeEnd.toEpochDay - cp.pointInTime.toEpochDay - TIMESTAMP_GRANULARITY_IN_DAYS
        if(!(countAfterInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0))
          println()
        assert(countAfterInDays % TIMESTAMP_GRANULARITY_IN_DAYS == 0)
        val countAfter = countAfterInDays / TIMESTAMP_GRANULARITY_IN_DAYS
        val result = EvidenceBasedWeightingScoreComputer.getCountForSameValueTransition(cp.curValueA,cp.curValueB,countAfter.toInt,isWildcard,
          transitionSets(vertexIdFirst),transitionSets(vertexIdSecond),nonInformativeValues,nonInformativeValuesIsStrict,Some(tfIDFTableAsMap(trainTimeEnd)))
        if(result.isDefined)
          totalCounts.addAll(result.get)
      }
    }
    totalCounts
  }

  def aggregateEventCounts(evaluationStepDurationInDays:Int,approxStatSampleSize:Int) = {
    val totalCounts = scala.collection.mutable.HashMap[LocalDate, EventOccurrenceStatistics]()
    val trainTimeEndsWithIndex = graph.trainTimeEnds.zipWithIndex
    val latestTime = graph.trainTimeEnds.max
    var processedEdges = 0
    val statPr = new PrintWriter(statFile)
    val adjacencyList = collection.mutable.HashMap[Int, collection.mutable.HashMap[Int, Seq[EventCounts]]]()
    val nEdges = graph.adjacencyList.map(_._2.size).sum
    graph.generalEdgeIterator.foreach { case (firstNode, secondNode, e, isEdgeInGraph) => {
      val totalCountsThisEdge = scala.collection.mutable.HashMap[LocalDate, EventOccurrenceStatistics]()
      val commonPointOfInterestIterator = new CommonPointOfInterestIterator(e.v1.roleLineage.toRoleLineage, e.v2.roleLineage.toRoleLineage)
      commonPointOfInterestIterator
        .withFilter(cp => !cp.prevPointInTime.isAfter(latestTime))
        .foreach(cp => {
          true==true
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
      if(processedEdges==0){
        val statRow = tuning.EdgeStatRowForTuning(e,totalCountsThisEdge.head._2,evaluationStepDurationInDays)
        statPr.println(statRow.getSchema.mkString(","))
      }
      if(nEdges < approxStatSampleSize || Random.nextDouble() < approxStatSampleSize / nEdges.toDouble){
        val statRows = totalCountsThisEdge.values.toIndexedSeq.map(v => tuning.EdgeStatRowForTuning(e,v,evaluationStepDurationInDays))
        statRows.sortBy(_.scoreStats.trainTimeEnd.toEpochDay).foreach(sr => statPr.println(sr.getStatRow.mkString(",")))
      }
      //add to SlimGraphSet:
      assert(firstNode<secondNode)
      val countsSorted = totalCountsThisEdge.toIndexedSeq.sortBy(_._1.toEpochDay)
        .map(t => EventCounts.from(t._2))
      val map = adjacencyList.getOrElseUpdate(firstNode,collection.mutable.HashMap[Int, Seq[EventCounts]]())
      map.put(secondNode,countsSorted)
      processedEdges+=1
      val toLog = LogUtil.buildLogProgressStrings(processedEdges,100000,None,"edges")
      if(toLog.isDefined)
        logger.debug(toLog.get)
    }}
    val trainTimeEndsSorted = graph.allEndTimes.toIndexedSeq.sortBy(_.toEpochDay)
    val graphSet = MemoryEfficientCompatiblityGraphSet(graph.verticesOrdered.map(_.id),trainTimeEndsSorted,adjacencyList)
    graphSet.toJsonFile(graphSetFile)
    statPr.close()
    totalCounts
  }

  private def addToTotal(totalCounts: mutable.HashMap[LocalDate, EventOccurrenceStatistics],
                         eventCounts: EventOccurrenceStatistics,
                         date:LocalDate) = {
    totalCounts.getOrElseUpdate(date, new EventOccurrenceStatistics(dsName, date))
      .addAll(eventCounts)
  }
}
object EvidenceBasedWeightingEventCounter {

}