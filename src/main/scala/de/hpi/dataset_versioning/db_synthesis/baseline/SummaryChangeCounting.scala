package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.TopDownMain.args
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import scala.collection.mutable

object SummaryChangeCounting extends App with StrictLogging{
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val ids = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  val uidToViewChanges:mutable.HashMap[String,ChangeStats] = mutable.HashMap()
  logger.debug(s"Running Database synthesis for ${ids.size} ids: $ids")
  val allAssociations:mutable.ArrayBuffer[AssociationSchema] = mutable.ArrayBuffer()
  val extraNonDecomposedViewTableChanges = mutable.HashMap[String,(Int,Int)]()
  ids.foreach(id => {
    var associations:Array[AssociationSchema] = null
    var tt:TemporalTable = null
    if(DBSynthesis_IOService.associationSchemataExist(subdomain,id)) {
      associations = AssociationSchema.loadAllAssociations(subdomain, id)
      allAssociations ++= associations
      //write sketches if not present:
      associations.foreach(a => {
        if(!DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(a.id).exists())
          println()
        assert(DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(a.id).exists())
      })
    } else if(!DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain,id)){
      if(tt==null)
        tt = TemporalTable.load(id)
      extraNonDecomposedViewTableChanges.put(id,countChanges(tt))
    }
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
  })
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
  val nChangesInAssociations =GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(
    uidToViewChanges.values
      .filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get))

  def countChanges(table: TemporalTable) = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(table)

  case class ChangeStats(nChangesInView:(Int,Int),nChangesInBCNFTables:Option[(Int,Int)],nChangesInAssociationTables:Option[(Int,Int)])

}
