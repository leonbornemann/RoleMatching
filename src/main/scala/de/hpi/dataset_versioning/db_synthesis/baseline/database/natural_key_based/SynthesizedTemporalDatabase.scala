package de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.ExecutedTableUnion
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.General_Many_To_Many_TupleMatching
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFSurrogateReferenceTable, BCNFTableSchema}

import scala.collection.mutable

class SynthesizedTemporalDatabase(associations: IndexedSeq[AssociationSchema],
                                  bcnfReferenceSchemata:collection.IndexedSeq[BCNFTableSchema],
                                  var curChangeCount:(Int,Int),
                                  val extraNonDecomposedViewTableChanges:Map[String,(Int,Int)]) extends StrictLogging{

  implicit class TuppleAdd(t: (Int, Int)) {
    def +(p: (Int, Int)) = (p._1 + t._1, p._2 + t._2)
  }

  //we assume that this method is called once the final database is assembled
  def generateQueries() = {
    //final database consists of finalSynthesizedTableIDs and allUnmatchedAssociations
    val viewToBCNF = bcnfReferenceSchemata.groupBy(_.id.viewID)
    viewToBCNF.foreach(id => {

    })
  }


  val bcnfSurrogateReferenceTables = bcnfReferenceSchemata
    .withFilter(s => !s.attributes.isEmpty)
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

  def sumChangeRange(values: Iterable[(Int, Int)]) = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(values)

  logger.debug(s"Undecomposed View tables: ${sumChangeRange(extraNonDecomposedViewTableChanges.values)}")
  logger.debug(s"Total (without associations): ${sumChangeRange(extraNonDecomposedViewTableChanges.values)}")
  logger.debug(s"Total (with associations): ${curChangeCount+sumChangeRange(extraNonDecomposedViewTableChanges.values)}")
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
    var nChangesInUnionedAssociations:(Int,Int) = (0,0)
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
    logger.debug(s"Total number of changes in final database: ${nChangesInUnionedAssociations +GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(extraNonDecomposedViewTableChanges.values)}")
    if(curChangeCount!=nChangesInUnionedAssociations)
      logger.debug(s"Warning: change counts do not match: $curChangeCount vs $nChangesInUnionedAssociations")
    //assert(curChangeCount==nChangesInUnionedAssociations)
  }

  def printChangeCounts() = {
    var nChanges:(Int,Int) = (0,0)
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

  def fixSurrogateReferences(unionedTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation,
                             oldTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation,
                             tupleMapping: mutable.HashMap[General_Many_To_Many_TupleMatching[Any], Int]) = {
    val correspondingReferenceTables = surrogateKeysToBCNFSchemata(oldTable.key.head)
    //create key mapping:
    val oldSKToNewSK = tupleMapping.flatMap{case (trs,indexInUnionTable) => {
      val newSurrogateKeyValue = unionedTable.rows(indexInUnionTable).keys
      trs.tupleReferences
        .filter(tr => tr.table==oldTable)
        .map(tr => (oldTable.rows(tr.rowIndex).keys.head,newSurrogateKeyValue.head))
    }}
    correspondingReferenceTables.foreach(bcnfRefereceTable => {
      val oldKeyIndex = bcnfRefereceTable.bcnfTableSchema.attributes.indexOf(oldTable.key.head)
      assert(oldKeyIndex != -1)
      bcnfRefereceTable.rows.foreach(r => {
        val old = r.associationReferences(oldKeyIndex)
        if(!oldSKToNewSK.contains(old))
          println()
        assert(oldSKToNewSK.contains(old))
        r.associationReferences(oldKeyIndex) = oldSKToNewSK(old)
      })
      bcnfRefereceTable.bcnfTableSchema.attributes(oldKeyIndex) = unionedTable.key.head
    })
  }

  def updateBCNFReferences(executedTableUnion: ExecutedTableUnion) = {
    assert(executedTableUnion.unionedSynthTable.unionedTables.size==2)
    val oldTable1 = executedTableUnion.matchForSynthUnion.firstMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]
    val oldTable2 = executedTableUnion.matchForSynthUnion.secondMatchPartner.asInstanceOf[SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]
    fixSurrogateReferences(executedTableUnion.unionedSynthTable,oldTable1,executedTableUnion.synthTupleMapping)
    fixSurrogateReferences(executedTableUnion.unionedSynthTable,oldTable2,executedTableUnion.synthTupleMapping)
  }

  def updateSynthesizedDatabase(executedTableUnion: ExecutedTableUnion) = {
    //get all the old BCNF tables and update their surrogate keys:
    assert(executedTableUnion.matchForSketch.tupleMapping.isDefined)
    val newSynthTable = executedTableUnion.unionedSynthTable
    val sketchMatch = executedTableUnion.matchForSketch
    updateBCNFReferences(executedTableUnion)
    newSynthTable.writeToStandardTemporaryFile()
    sketchToSynthTableID.put(executedTableUnion.unionedTableSketch,newSynthTable.uniqueSynthTableID)
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
    logger.debug(s"Reducing changes by ${sketchMatch.changeBenefit}")
    curChangeCount = (curChangeCount._1 - sketchMatch.changeBenefit._1,curChangeCount._2+sketchMatch.changeBenefit._2)
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
