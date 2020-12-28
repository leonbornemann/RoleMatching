package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable

class TopDown(subdomain:String,loadFilteredAssociationsOnly:Boolean,idsToIgnore:Set[String]=Set()) extends StrictLogging{

  def synthesizeDatabase(countChangesForAllSteps:Boolean = true):Unit = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    synthesizeDatabase(subdomainIds,countChangesForAllSteps)
  }

  def countChanges(table: TemporalTable) = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(table)

  def synthesizeDatabase(ids: IndexedSeq[String], countChangesForAllSteps: Boolean):Unit = {
    val uidToViewChanges:mutable.HashMap[String,ChangeStats] = mutable.HashMap()
    logger.debug(s"Running Database synthesis for ${ids.size} ids: $ids")
    val allAssociations:mutable.ArrayBuffer[AssociationSchema] = mutable.ArrayBuffer()
    val extraNonDecomposedViewTableChanges = mutable.HashMap[String,(Int,Int)]()
    val associationsWithChanges:Set[DecomposedTemporalTableIdentifier] = if(loadFilteredAssociationsOnly) DecomposedTemporalTableIdentifier.loadAllAssociationsWithChanges().toSet else Set()
    ids.foreach(id => {
      var associations:Array[AssociationSchema] = null
      var tt:TemporalTable = null
      if(DBSynthesis_IOService.associationSchemataExist(subdomain,id)) {
        associations = AssociationSchema.loadAllAssociations(subdomain, id)
        allAssociations ++= associations
          .filter(a => !loadFilteredAssociationsOnly || associationsWithChanges.contains(a.id))
        //write sketches if not present:
        val missing = associations.filter(a => !DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(a.id).exists())
//        missing.foreach(a => println(s"${a.id}  $a"))
//        if(id=="72qm-3bwf"){
//          println("ahaa")
//          println(missing.size)
//          associations.foreach(println(_))
//
//        }
        associations.foreach(a => {
          if(!DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(a.id).exists())
            println()
          assert(DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(a.id).exists())
        })
      } else if(!DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain,id) && countChangesForAllSteps){
        if(tt==null)
          tt = TemporalTable.load(id)
        extraNonDecomposedViewTableChanges.put(id,countChanges(tt))
      }
      if(countChangesForAllSteps) {
        if(tt==null)
          tt = TemporalTable.load(id)
        var bcnfChangeCount: Option[(Int,Int)] = None
        var associationChangeCount: Option[(Int,Int)] = None
        if (!DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain, id)) {
          logger.debug(s"no decomposed Temporal tables found for $id, skipping this")
        } else {
          val dtts = SurrogateBasedDecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
          bcnfChangeCount = Some(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(dtts.map(dtt => GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(TemporalTable.loadBCNFFromStandardBinaryFile(dtt.id)))))
        }
        if(!DBSynthesis_IOService.associationSchemataExist(subdomain, id)) {
          logger.debug(s"no decomposed Temporal associations found for $id, skipping this")
        } else{
          associationChangeCount = Some(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(associations.map(a => GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(a.id)))))
        }
        uidToViewChanges.put(id, ChangeStats(countChanges(tt), bcnfChangeCount, associationChangeCount))
      }
    })
    if (countChangesForAllSteps) {
      logger.debug(s"extra Non-decomposed temporal table changes (should be zero): ${GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(extraNonDecomposedViewTableChanges.values)}")
      val nChangesInViewSet = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(uidToViewChanges.values.map(_.nChangesInView))
      logger.debug(s"number of changes in view set, where normalization result exists: $nChangesInViewSet")
      val nChangesInBCNFTables = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(uidToViewChanges.values.filter(_.nChangesInBCNFTables.isDefined).map(_.nChangesInBCNFTables.get))
      logger.debug(s"number of changes in BCNF tables: $nChangesInBCNFTables")
//      logger.debug(s"total number of changes in this step: ${nChangesInBCNFTables+extraNonDecomposedViewTableChanges.values.sum}")
//      val nChangesInAssociations = uidToViewChanges.values.filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get).reduce(_ + _)
//      logger.debug(s"number of changes in associations: $nChangesInAssociations")
//      logger.debug(s"extra changes for BCNF tables with no associations: ${uidToViewChanges.filter(cs => cs._2.nChangesInAssociationTables.isEmpty && cs._2.nChangesInBCNFTables.isDefined)
//      .map(_._2.nChangesInBCNFTables.get).sum}")
      //logger.debug(s"total number of changes in this step: ${nChangesInAssociations+numberOfChangesInTablesWithNoDTTORAssociation}")
    }
    val nChangesInAssociations = if(countChangesForAllSteps) GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(
      uidToViewChanges.values
      .filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get)) else (-1,-1)
    if(!countChangesForAllSteps) {
      logger.debug("Not counting all changes, thus initializing topdown optimizer with a dummy initial change value")
    }
    //TODO: use this:
    val bcnfSchemata:IndexedSeq[BCNFTableSchema] = IndexedSeq()//ids.flatMap(id => BCNFTableSchema.loadAllBCNFTableSchemata(subdomain,id))
    val topDownOptimizer = new TopDownOptimizer(allAssociations.toIndexedSeq,
      bcnfSchemata,
      nChangesInAssociations,
      extraNonDecomposedViewTableChanges.toMap)
    topDownOptimizer.optimize()
  }

  case class ChangeStats(nChangesInView:(Int,Int),nChangesInBCNFTables:Option[(Int,Int)],nChangesInAssociationTables:Option[(Int,Int)])

}
object TopDown extends StrictLogging{
  logger.debug("Currently passing empty set as primary key to temporal tables!")
}
