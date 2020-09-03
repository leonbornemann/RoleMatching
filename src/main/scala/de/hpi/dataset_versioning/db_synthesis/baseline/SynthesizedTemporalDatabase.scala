package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class SynthesizedTemporalDatabase(associations: IndexedSeq[DecomposedTemporalTable],
                                  allAssociationSketches:mutable.HashSet[SynthesizedTemporalDatabaseTableSketch],
                                  var curChangeCount:Long) extends StrictLogging{

  def writeToStandardFiles() = {
    var nChanges = 0
    finalSynthesizedTableIDs.foreach(id => {
      val synthTable = SynthesizedTemporalDatabaseTable.loadFromStandardFile(id)
      val changesInThisTable = synthTable.numChanges
      nChanges += changesInThisTable
      logger.debug(s"writing table ${synthTable.informativeTableName} to file with id $id (#changes:$changesInThisTable")
      synthTable.writeToStandardTemporaryFile()
    })
    //all remaining associations are serialized as-is:
    logger.debug(s"Serializing ${allUnmatchedAssociations.size} remaining unmatched associations as-is")
    allUnmatchedAssociations.values.foreach(a => {
      val asSynthTable = SynthesizedTemporalDatabaseTable.initFrom(a)
      val changesInThisTable = asSynthTable.numChanges
      nChanges += changesInThisTable
      logger.debug(s"writing table ${asSynthTable.informativeTableName} to file with id ${asSynthTable.uniqueSynthTableID} (#changes:$changesInThisTable)")
    })
    logger.debug(s"Final Database has $nChanges number of changes")
    logger.debug(s"During synthesis we recorder the number of changes to be $curChangeCount")
    assert(curChangeCount==nChanges)
  }


  def printState() = {
    logger.debug("Current DAtabase state")
    logger.debug(s"Synthesized tables: ${finalSynthesizedTableIDs.toIndexedSeq.sorted.mkString(",")}")
    logger.debug(s"unmatched associations: ${allUnmatchedAssociations.map(_._2.compositeID).mkString("  ,  ")}")
    logger.debug(s"current number of changes: $curChangeCount")
    //writeToStandardFiles()
    //TODO: run and check if final result is correct
    //TODO: serialize final result!
  }


  private val allUnmatchedAssociations = mutable.HashMap() ++ associations.map(a => (a.id,a)).toMap

  private val dttByID = associations.map(a => (a.id,a)).toMap
  var sketchToSynthTableID = mutable.HashMap[SynthesizedTemporalDatabaseTableSketch,Int]()
  var finalSynthesizedTableIDs = mutable.HashSet[Int]()


  def updateSynthesizedDatabase(newSynthTable: SynthesizedTemporalDatabaseTable,
                                newSynthTableSketch: SynthesizedTemporalDatabaseTableSketch,
                                executedMatch:TableUnionMatch[Int]) = {
    sketchToSynthTableID.put(newSynthTableSketch,newSynthTable.uniqueSynthTableID)
    finalSynthesizedTableIDs += newSynthTable.uniqueSynthTableID
    //remove old ids:
    val removed = newSynthTable.unionedTables.map(allUnmatchedAssociations.remove(_))
      .filter(_.isDefined)
      .size
    logger.debug(s"Removed $removed tables from unmatched Associations as they are now matched to a synthesized table")
    val sketchA = executedMatch.firstMatchPartner.asInstanceOf[SynthesizedTemporalDatabaseTableSketch]
    val sketchB = executedMatch.secondMatchPartner.asInstanceOf[SynthesizedTemporalDatabaseTableSketch]
    removeOldSynthIDIFUnion(sketchA)
    removeOldSynthIDIFUnion(sketchB)
    //table serialization
    logger.debug(s"Executed Match between ${executedMatch.firstMatchPartner.informativeTableName} and ${executedMatch.secondMatchPartner.informativeTableName}")
    logger.debug(s"Reducing changes by ${executedMatch.score}")
    newSynthTable.writeToStandardTemporaryFile()
    curChangeCount -= executedMatch.score
  }


  private def removeOldSynthIDIFUnion(sketchA: SynthesizedTemporalDatabaseTableSketch) = {
    if (sketchA.isTrueUnion) {
      assert(sketchToSynthTableID.contains(sketchA))
      val idToRemove = sketchToSynthTableID(sketchA)
      finalSynthesizedTableIDs.remove(idToRemove)
      logger.debug(s"removed synthesized table $idToRemove as this was now unioned to a new synthesized database table")
    }
  }

  def loadSynthesizedTable(sketchA: SynthesizedTemporalDatabaseTableSketch) = {
    if (sketchA.unionedTables.size == 1) {
      assert(!sketchToSynthTableID.contains(sketchA))
      //read from original:
      SynthesizedTemporalDatabaseTable.initFrom(dttByID(sketchA.unionedTables.head))
    } else {
      assert(sketchToSynthTableID.contains(sketchA))
      SynthesizedTemporalDatabaseTable.loadFromStandardFile(sketchToSynthTableID(sketchA))
    }
  }

}
