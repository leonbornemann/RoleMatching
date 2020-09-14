package de.hpi.dataset_versioning.db_synthesis.baseline

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MostDistinctTimestampIndexBuilder[A](unmatchedAssociations: collection.Set[TemporalDatabaseTableTrait[A]]) extends StrictLogging{

  assert(unmatchedAssociations.forall(_.isAssociation))

  val N_TIMESTAMPS_IN_INDEX = Math.min(3,IOService.STANDARD_TIME_RANGE.size)

  def buildTableIndexOnNonKeyColumns() = {
    val attributesOnWhichToIndex = unmatchedAssociations.flatMap(ua => ua.nonKeyAttributeLineages.map(al => (ua,al)))
    val nonCoveredAttributeIDs = mutable.HashSet() ++ attributesOnWhichToIndex.map(t => (t._1,t._2.attrId)).toSet
    val chosenTimestamps = mutable.ArrayBuffer[LocalDate]()
    while(chosenTimestamps.size<=N_TIMESTAMPS_IN_INDEX && !nonCoveredAttributeIDs.isEmpty){
      val chosen = getNextMostDiscriminatingTimestamp(chosenTimestamps,attributesOnWhichToIndex,nonCoveredAttributeIDs)
      chosenTimestamps += chosen
    }
    val layeredTableIndex = new LayeredTupleIndex[A](chosenTimestamps,unmatchedAssociations.map(a => {
      val nonKeyAttr = a.nonKeyAttributeLineages.head
      val indexOfNonKeyAttr = a.columns.zipWithIndex.filter(_._1.attributeLineage.attrId==nonKeyAttr.attrId).head._2
      (a,indexOfNonKeyAttr)
    }))
    layeredTableIndex
  }

  private def getNextMostDiscriminatingTimestamp(chosenTimestamps: ArrayBuffer[LocalDate],
                                                 attributesOnWhichToIndex: collection.Set[(TemporalDatabaseTableTrait[A], AttributeLineage)],
                                                 nonCoveredAttributeIDs: mutable.HashSet[(TemporalDatabaseTableTrait[A], Int)]) = {
    val timerange = IOService.STANDARD_TIME_RANGE
    val byTimestamp = timerange
      .withFilter(!chosenTimestamps.contains(_))
      .map(t => {
        val (isNonWildCard, isWildCard) = attributesOnWhichToIndex
          .filter(al => nonCoveredAttributeIDs.contains((al._1,al._2.attrId)))
          .partition(al => al._2.valueAt(t)._2.exists)
        (t, isNonWildCard, isWildCard)
      })
    val bestNextTs = byTimestamp.sortBy(-_._2.size)
      .head
    val nowCovered = bestNextTs._2.map(al => (al._1, al._2.attrId)).toSet
    nowCovered.foreach(nonCoveredAttributeIDs.remove(_))
    logger.debug(s"${bestNextTs._1} covers ${bestNextTs._2.size} new attributes leaving ${bestNextTs._3.size} still open")
    bestNextTs._1
  }
}
