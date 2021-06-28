package de.hpi.tfm.fact_merging.metrics

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{ChangePoint, CommonPointOfInterestIterator, ValueTransition}
import de.hpi.tfm.evaluation.data.SlimGraphWithoutWeight
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScoreComputer

import java.time.LocalDate

class MultipleEvenWeightStatCounter(dsName:String,graph:SlimGraphWithoutWeight, TIMESTAMP_GRANULARITY_IN_DAYS:Int,nonInformativeValuesIsStrict:Boolean =false) extends StrictLogging{

  val isWildcard:(Any => Boolean) = graph.verticesOrdered.head.factLineage.toFactLineage.isWildcard
  val transitionSets = graph.verticesOrdered.zipWithIndex
    .map(il => (il._2,il._1.factLineage.toFactLineage.valueTransitions(true,false)))
    .toMap
  val nonInformativeValues = GLOBAL_CONFIG.nonInformativeValues

  def getEventCounts(cp: ChangePoint[Any],vertexIdFirst:Int,vertexIdSecond:Int) = {
    val totalCounts = new MultipleEventWeightScoreOccurrenceStats(null,null)
    val countPrev = MultipleEventWeightScoreComputer.getCountPrev(cp,TIMESTAMP_GRANULARITY_IN_DAYS).toInt
    val countPrevTransiton = MultipleEventWeightScoreComputer.getCountForSameValueTransition(cp.prevValueA,cp.prevValueB,countPrev,isWildcard,
      transitionSets(vertexIdFirst),transitionSets(vertexIdSecond),nonInformativeValues,nonInformativeValuesIsStrict)
    val countCurrent = MultipleEventWeightScoreComputer.getCountForTransition(cp,isWildcard,
      transitionSets(vertexIdFirst),transitionSets(vertexIdSecond),nonInformativeValues,nonInformativeValuesIsStrict)
    totalCounts.addAll(countPrevTransiton)
    totalCounts.addAll(countCurrent)
    totalCounts
  }

  def aggregateEventCounts() = {
    val counts = scala.collection.mutable.HashMap[LocalDate, MultipleEventWeightScoreOccurrenceStats]()
    val trainTimeEndsWithIndex = graph.trainTimeEnds.zipWithIndex
    val latestTime = graph.trainTimeEnds.max
    graph.generalEdgeIterator.foreach { case (firstNode, secondNode, e, isEdgeInGraph) => {
      val commonPointOfInterestIterator = new CommonPointOfInterestIterator[Any](e.v1.factLineage.toFactLineage, e.v2.factLineage.toFactLineage)
      commonPointOfInterestIterator
        .withFilter(cp => !cp.pointInTime.isAfter(latestTime))
        .foreach(cp => {
          val eventCounts = getEventCounts(cp, firstNode, secondNode)
          if (!cp.pointInTime.isAfter(graph.smallestTrainTimeEnd)) {
            counts.getOrElseUpdate(graph.smallestTrainTimeEnd, new MultipleEventWeightScoreOccurrenceStats(dsName: String, graph.smallestTrainTimeEnd))
              .addAll(eventCounts)
          }
          trainTimeEndsWithIndex.foreach { case (ld, i) => {
            if (isEdgeInGraph(i)) {
              counts.getOrElseUpdate(ld, new MultipleEventWeightScoreOccurrenceStats(dsName, ld))
                .addAll(eventCounts)
            }
          }
          }
        })
    }
    }
    counts
  }

}
