package de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFSurrogateReferenceTable, BCNFTableSchema}
import de.hpi.dataset_versioning.db_synthesis.sketches.table.SynthesizedTemporalDatabaseTableSketch
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable

class SynthesizedTemporalDatabase(associations: IndexedSeq[AssociationSchema],
                                  bcnfReferenceSchemata:collection.IndexedSeq[BCNFTableSchema],
                                  var curChangeCount:Long,
                                  val extraNonDecomposedViewTableChanges:Map[String,Long]) extends StrictLogging{

  val bcnfSurrogateReferenceTables = bcnfReferenceSchemata
    .map(s => BCNFSurrogateReferenceTable.loadFromStandardOptimizationInputFile(s.id))
    .map(t => (t.bcnfTableSchema.id,t))
    .toMap
  val surrogateKeysToBCNFSchemata = bcnfSurrogateReferenceTables.values
    .flatMap(bcnf => bcnf.bcnfTableSchema.attributes.map(a => (a,Set(bcnf))))
    .toMap


  private val allUnmatchedAssociations = mutable.HashMap() ++ associations.map(a => (a.id,a)).toMap
  var sketchToSynthTableID = mutable.HashMap[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch,Int]()
  var finalSynthesizedTableIDs = mutable.HashSet[Int]()

  //tables which we can't optimize with our approach:
  //convert the initial tables to synth tables and serialize them:
  logger.debug(s"Initialized database with ${associations.size} associations and ${extraNonDecomposedViewTableChanges.size} non-decomposed views")
  logger.debug("Initial change counts:")
  logger.debug(s"Associations: ${curChangeCount}")
  logger.debug(s"Undecomposed View tables: ${extraNonDecomposedViewTableChanges.values.sum}")
  logger.debug(s"Total (without associations): ${extraNonDecomposedViewTableChanges.values.sum}")
  logger.debug(s"Total (with associations): ${curChangeCount+extraNonDecomposedViewTableChanges.values.sum}")
  logger.debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
  logger.debug("Initialized with the following bcnf tables:")
  bcnfSurrogateReferenceTables.foreach(bcnf => {
    logger.debug(bcnf._2.toString)
  })
  logger.debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
  logger.debug("Initialized with the following associations:")
  associations.foreach(a => logger.debug(a.toString))
  logger.debug("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")


  def standardChangeCount(synthTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation) = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(synthTable)

  def writeToStandardFiles() = {
    var nChangesInUnionedAssociations:Long = 0
    finalSynthesizedTableIDs.foreach(id => {
      val synthTable = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromSynthDatabaseTableFile(id)
      val changesInThisTable = standardChangeCount(synthTable)
      nChangesInUnionedAssociations += changesInThisTable
      logger.debug(s"writing table ${synthTable} to file with id $id (#changes:$changesInThisTable")
      synthTable.writeToStandardTemporaryFile()
    })
    //all remaining associations are serialized as-is:
    logger.debug(s"Serializing ${allUnmatchedAssociations.size} remaining unmatched associations as-is")
    allUnmatchedAssociations.values
      .foreach(a => {
        val asSynthTable = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(a.id)
        val changesInThisTable = standardChangeCount(asSynthTable)
        nChangesInUnionedAssociations += changesInThisTable
        logger.debug(s"writing table ${asSynthTable} to file with id ${asSynthTable.uniqueSynthTableID} (#changes:$changesInThisTable)")
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
    logger.debug(s"unmatched associations: ${allUnmatchedAssociations.values.mkString("; ")}")
    logger.debug(s"current number of changes: $curChangeCount")
  }

  def fixSurrogateReferences(unionedTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, oldTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation) = {
    val correspondingReferenceTables = surrogateKeysToBCNFSchemata(oldTable.key.head)
    //create key mapping:
    val valueToKeyNew = unionedTable.rows.map(r => (r.valueLineage,r.keys.head)).toMap
    val oldSKToNewSK = oldTable.rows.map(r => (r.keys.head,valueToKeyNew(r.valueLineage))).toMap
    correspondingReferenceTables.foreach(bcnfRefereceTable => {
      val oldKeyIndex = bcnfRefereceTable.bcnfTableSchema.attributes.indexOf(oldTable.key.head)
      assert(oldKeyIndex != -1)
      bcnfRefereceTable.rows.foreach(r => {
        val old = r.associationReferences(oldKeyIndex)
        assert(oldSKToNewSK.contains(old))
        r.associationReferences(oldKeyIndex) = oldSKToNewSK(old)
      })
      bcnfRefereceTable.bcnfTableSchema.attributes(oldKeyIndex) = unionedTable.key.head
    })
  }

  def updateBCNFReferences(unionedTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, executedMatch: TableUnionMatch[Any]) = {
    val newSurrogateKeyID = unionedTable.key.head
    assert(unionedTable.unionedTables.size==2)
    val oldTable1 = executedMatch.firstMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]
    val oldTable2 = executedMatch.firstMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]
    fixSurrogateReferences(unionedTable,oldTable1)
    fixSurrogateReferences(unionedTable,oldTable2)
  }

  def updateSynthesizedDatabase(newSynthTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation,
                                newSynthTableSketch: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch,
                                realDataMatch:TableUnionMatch[Any],
                                sketchMatch:TableUnionMatch[Int]) = {
    //get all the old BCNF tables and update their surrogate keys:
    assert(sketchMatch.tupleMapping.isDefined)
    updateBCNFReferences(newSynthTable,realDataMatch)

    newSynthTable.writeToStandardTemporaryFile()
    sketchToSynthTableID.put(newSynthTableSketch,newSynthTable.uniqueSynthTableID)
    finalSynthesizedTableIDs += newSynthTable.uniqueSynthTableID
    //remove old ids:
    val removed = newSynthTable.getUnionedOriginalTables.map(allUnmatchedAssociations.remove(_))
      .filter(_.isDefined)
      .size
    val sketchA = sketchMatch.firstMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
    val sketchB = sketchMatch.secondMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch]
    removeOldSynthIDIFUnion(sketchA)
    removeOldSynthIDIFUnion(sketchB)
    //table serialization
    logger.debug(s"Executed Match between ${sketchMatch.firstMatchPartner} {${sketchMatch.firstMatchPartner.nrows} rows} and ${sketchMatch.secondMatchPartner} {${sketchMatch.secondMatchPartner.nrows} rows}, new row count: {${newSynthTable.nrows}}")
    logger.debug(s"Reducing changes by ${sketchMatch.score}")
    curChangeCount -= sketchMatch.score
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
    if (sketchA.getUnionedOriginalTables.size == 1) {
      assert(!sketchToSynthTableID.contains(sketchA))
      //read from original:
      SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(sketchA.getUnionedOriginalTables.head)
    } else {
      assert(sketchToSynthTableID.contains(sketchA))
      SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromSynthDatabaseTableFile(sketchToSynthTableID(sketchA))
    }
  }

}
