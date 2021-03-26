package de.hpi.tfm.compatibility.index

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.AttributeLineage
import de.hpi.tfm.data.tfmp_input.table.TemporalDatabaseTableTrait
import de.hpi.tfm.io.IOService

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MostDistinctTimestampIndexBuilder[A](unmatchedAssociations: collection.Set[TemporalDatabaseTableTrait[A]],enableLogging:Boolean=true) extends StrictLogging{

  assert(unmatchedAssociations.forall(_.isAssociation))

  val indexSize = 1

  def buildTableIndexOnNonKeyColumns() = {
    val attributesOnWhichToIndex = unmatchedAssociations.flatMap(ua => ua.dataAttributeLineages.map(al => (ua,al)))
    val nonCoveredAttributeIDs = mutable.HashSet() ++ attributesOnWhichToIndex.map(t => (t._1,t._2.attrId)).toSet
    val chosenTimestamps = mutable.ArrayBuffer[LocalDate]()
    while(chosenTimestamps.size<indexSize && !nonCoveredAttributeIDs.isEmpty){
      val chosen = getNextMostDiscriminatingTimestamp(chosenTimestamps,attributesOnWhichToIndex,nonCoveredAttributeIDs)
      chosenTimestamps += chosen
    }
    if(enableLogging)
      logger.debug("Done Selecting timestamps, beginning to build index")
    val layeredTableIndex = new LayeredTupleIndex[A](chosenTimestamps,unmatchedAssociations.map(a => {
      val nonKeyAttr = a.dataAttributeLineages.head
      (a,0) //TODO: 0 is the index of the column - originally this was designed for multi-column tables, which is why this is an ugly artifact of the past - fix it when we have the time!
    }))
    if(enableLogging)
      logger.debug("Finished building index")
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
    if(enableLogging)
      logger.debug(s"${bestNextTs._1} covers ${bestNextTs._2.size} new attributes leaving ${bestNextTs._3.size} still open")
    bestNextTs._1
  }
}
