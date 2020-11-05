package de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.database.query_tracking.ViewQueryTracker
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.db_synthesis.sketches.table.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class SynthesizedTemporalDatabase(associations: IndexedSeq[AssociationSchema],
                                  bcnfReferenceSchemata:collection.IndexedSeq[BCNFTableSchema],
                                  var curChangeCount:Long,
                                  val extraNonDecomposedViewTableChanges:Map[String,Long],
                                  tracker:Option[ViewQueryTracker] = None) extends StrictLogging{

  //tables which we can't optimize with our approach:
  logger.debug("Initilializing extra BCNF tables")
  //convert the initial tables to synth tables and serialize them:
  logger.debug(s"Initialized database with ${associations.size} associations and ${extraNonDecomposedViewTableChanges.size} non-decomposed views")
  logger.debug("Initial change counts:")
  logger.debug(s"Associations: ${curChangeCount}")
  logger.debug(s"Undecomposed View tables: ${extraNonDecomposedViewTableChanges.values.sum}")
  logger.debug(s"Total (without associations): ${extraNonDecomposedViewTableChanges.values.sum}")
  logger.debug(s"Total (with associations): ${curChangeCount+extraNonDecomposedViewTableChanges.values.sum}")

  def standardChangeCount(synthTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation) = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(synthTable)

  def writeToStandardFiles() = {
    var nChangesInUnionedAssociations:Long = 0
    finalSynthesizedTableIDs.foreach(id => {
      val synthTable = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromSynthDatabaseTableFile(id)
      val changesInThisTable = standardChangeCount(synthTable)
      nChangesInUnionedAssociations += changesInThisTable
      logger.debug(s"writing table ${synthTable.informativeTableName} to file with id $id (#changes:$changesInThisTable")
      synthTable.writeToStandardTemporaryFile()
    })
    //all remaining associations are serialized as-is:
    logger.debug(s"Serializing ${allUnmatchedAssociations.size} remaining unmatched associations as-is")
    allUnmatchedAssociations.values
      .foreach(a => {
        val asSynthTable = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(a.id)
        val changesInThisTable = standardChangeCount(asSynthTable)
        nChangesInUnionedAssociations += changesInThisTable
        logger.debug(s"writing table ${asSynthTable.informativeTableName} to file with id ${asSynthTable.uniqueSynthTableID} (#changes:$changesInThisTable)")
      })
    logger.debug(s"Final Database has $nChangesInUnionedAssociations number of changes in associations")
    logger.debug(s"During synthesis we recorded the number of changes in associations to be $curChangeCount")
    logger.debug(s"Total number of changes in final database: ${nChangesInUnionedAssociations +extraNonDecomposedViewTableChanges.values.sum}")
    if(curChangeCount!=nChangesInUnionedAssociations)
      println()
    assert(curChangeCount==nChangesInUnionedAssociations)
  }

  def printChangeCounts() = {
    var nChanges:Long = 0
    finalSynthesizedTableIDs.foreach(id => {
      val synthTable = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromSynthDatabaseTableFile(id)
      val changesInThisTable = standardChangeCount(synthTable)
      nChanges += changesInThisTable
    })
    allUnmatchedAssociations.values
      .foreach(a => {
        val asSynthTable = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(a.id)
        val changesInThisTable = standardChangeCount(asSynthTable)
        nChanges += changesInThisTable
      })
    println(s"tracked $curChangeCount real: $nChanges")
  }

  def printState() = {
    logger.debug("Current DAtabase state")
    logger.debug(s"Synthesized tables: ${finalSynthesizedTableIDs.toIndexedSeq.sorted.mkString(",")}")
    logger.debug(s"unmatched associations: ${allUnmatchedAssociations.map(_._2.compositeID).mkString("  ,  ")}")
    logger.debug(s"current number of changes: $curChangeCount")
  }

  private val allUnmatchedAssociations = mutable.HashMap() ++ associations.map(a => (a.id,a)).toMap

  private val dttByID = associations.map(a => (a.id,a)).toMap
  var sketchToSynthTableID = mutable.HashMap[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch,Int]()
  var finalSynthesizedTableIDs = mutable.HashSet[Int]()


  def updateSynthesizedDatabase(newSynthTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation,
                                newSynthTableSketch: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch,
                                executedMatch:TableUnionMatch[Int]) = {
    newSynthTable.writeToStandardTemporaryFile()
    sketchToSynthTableID.put(newSynthTableSketch,newSynthTable.uniqueSynthTableID)
    finalSynthesizedTableIDs += newSynthTable.uniqueSynthTableID
    //remove old ids:
    val removed = newSynthTable.getUnionedTables.map(allUnmatchedAssociations.remove(_))
      .filter(_.isDefined)
      .size
    logger.debug(s"Removed $removed tables from unmatched Associations as they are now matched to a synthesized table")
    val sketchA = executedMatch.firstMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
    val sketchB = executedMatch.secondMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
    removeOldSynthIDIFUnion(sketchA)
    removeOldSynthIDIFUnion(sketchB)
    //table serialization
    logger.debug(s"Executed Match between ${executedMatch.firstMatchPartner.informativeTableName} and ${executedMatch.secondMatchPartner.informativeTableName}")
    logger.debug(s"Reducing changes by ${executedMatch.score}")
    curChangeCount -= executedMatch.score
    if(tracker.isDefined){
      tracker.get.updateForSynthTable(newSynthTable,executedMatch)
    }
  }


  private def removeOldSynthIDIFUnion(sketchA: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = {
    if (sketchA.isTrueUnion) {
      assert(sketchToSynthTableID.contains(sketchA))
      val idToRemove = sketchToSynthTableID(sketchA)
      finalSynthesizedTableIDs.remove(idToRemove)
      logger.debug(s"removed synthesized table $idToRemove as this was now unioned to a new synthesized database table")
    }
  }

  def loadSynthesizedTable(sketchA: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = {
    if (sketchA.getUnionedTables.size == 1) {
      assert(!sketchToSynthTableID.contains(sketchA))
      //read from original:
      SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(sketchA.getUnionedTables.head)
    } else {
      assert(sketchToSynthTableID.contains(sketchA))
      SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromSynthDatabaseTableFile(sketchToSynthTableID(sketchA))
    }
  }

}
