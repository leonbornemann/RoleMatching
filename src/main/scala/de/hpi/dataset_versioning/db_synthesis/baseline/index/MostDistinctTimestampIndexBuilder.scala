package de.hpi.dataset_versioning.db_synthesis.baseline.index

import java.io.PrintWriter
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class MostDistinctTimestampIndexBuilder[A](unmatchedAssociations: collection.Set[TemporalDatabaseTableTrait[A]]) extends StrictLogging{

  assert(unmatchedAssociations.forall(_.isAssociation))

  val indexSize = Math.min(GLOBAL_CONFIG.INDEX_DEPTH,IOService.STANDARD_TIME_RANGE.size)

  def exportIndexStats(layeredTableIndex: LayeredTupleIndex[A]) = {
    val fileTuples = new PrintWriter("tuplesPerGroup.txt")
    val fileTables = new PrintWriter("tablesPerGroup.txt")
    val tuplePerTableFile = new PrintWriter("tuplePerTable.txt")
    layeredTableIndex.tupleGroupIterator.foreach(g => {
      val nTables = g.map(_._1).toSet.size
      fileTables.println(nTables)
      fileTuples.println(g.size)
      tuplePerTableFile.println(g.size / nTables.toDouble)
    })
    fileTables.close()
    fileTables.close()
    tuplePerTableFile.close()
  }

  def buildTableIndexOnNonKeyColumns() = {
    val attributesOnWhichToIndex = unmatchedAssociations.flatMap(ua => ua.attributeLineages.map(al => (ua,al)))
    val nonCoveredAttributeIDs = mutable.HashSet() ++ attributesOnWhichToIndex.map(t => (t._1,t._2.attrId)).toSet
    val chosenTimestamps = mutable.ArrayBuffer[LocalDate]()
    while(chosenTimestamps.size<indexSize && !nonCoveredAttributeIDs.isEmpty){
      val chosen = getNextMostDiscriminatingTimestamp(chosenTimestamps,attributesOnWhichToIndex,nonCoveredAttributeIDs)
      chosenTimestamps += chosen
    }
    logger.debug("Done Selecting timestamps, beginning to build index")
    val layeredTableIndex = new LayeredTupleIndex[A](chosenTimestamps,unmatchedAssociations.map(a => {
      val nonKeyAttr = a.attributeLineages.head
      val indexOfNonKeyAttr = a.columns.zipWithIndex.filter(_._1.attributeLineage.attrId==nonKeyAttr.attrId).head._2
      (a,indexOfNonKeyAttr)
    }))
    logger.debug("Finished building index")
    exportIndexStats(layeredTableIndex)
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
