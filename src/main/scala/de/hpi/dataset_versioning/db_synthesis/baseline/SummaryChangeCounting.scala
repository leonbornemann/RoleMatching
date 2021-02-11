package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.TopDownMain.args
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import scala.collection.mutable

object SummaryChangeCounting extends App with StrictLogging{
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val maybeIds:Seq[String] = if (args.size>2) args(2).split(",") else Seq()
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val ids = if(maybeIds.size==0) subDomainInfo(subdomain).map(_.id).toIndexedSeq else maybeIds
  val uidToViewChanges:mutable.HashMap[String,ChangeStats] = mutable.HashMap()
  logger.debug(s"Running Database synthesis for ${ids.size} ids: $ids")
  var nFieldsInViewSet = 0
  var nFieldsInAssociations = 0
  var missingBCNFTables = 0
  ids.foreach(id => {
    val tt = TemporalTable.load(id)
    assert(AssociationSchema.associationSchemataExist(subdomain,id))
    val associations = AssociationSchema.loadAllAssociations(subdomain, id)
    assert(BCNFTableSchema.decomposedTemporalTablesExist(subdomain, id))
    val dtts = SurrogateBasedDecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
    tt.addSurrogates(dtts.flatMap(_.surrogateKey).toSet)
    dtts
      .filter(_.attributes.size>0)
      .foreach(dtt => {
      if(!TemporalTable.bcnfContentTableExists(dtt.id)){
        val projection = tt.project(dtt).projection
        projection.writeTOBCNFTemporalTableFile
      }
    })
    val bcnfChangeCount = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(dtts.map(dtt => {
      if(!TemporalTable.bcnfContentTableExists(dtt.id)){
        missingBCNFTables+=1
        (0,0)
      } else
        GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(TemporalTable.loadBCNFFromStandardBinaryFile(dtt.id))
    }))
    val associationChangeCounts = associations.map(a => {
      val association = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(a.id)
      nFieldsInAssociations += association.nrows
      GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(association)
    })
    val associationChangeCount = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(associationChangeCounts)
    uidToViewChanges.put(id, ChangeStats(countChanges(tt), bcnfChangeCount, associationChangeCount))
    nFieldsInViewSet += tt.rows.size*tt.attributes.size
  })
  logger.debug(s"number of missing bcnf tables: $missingBCNFTables")
  logger.debug(s"number of fields in view set: $nFieldsInViewSet")
  val nChangesInViewSet = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRangesAsLong(uidToViewChanges.values.map(_.nChangesInView))
  logger.debug(s"number of changes in view set, where normalization result exists: $nChangesInViewSet")
  val nChangesInBCNFTables = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRangesAsLong(uidToViewChanges.values.map(_.nChangesInBCNFTables))
  logger.debug(s"number of changes in BCNF tables: $nChangesInBCNFTables")
  //      logger.debug(s"total number of changes in this step: ${nChangesInBCNFTables+extraNonDecomposedViewTableChanges.values.sum}")
  //      val nChangesInAssociations = uidToViewChanges.values.filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get).reduce(_ + _)
  //      logger.debug(s"number of changes in associations: $nChangesInAssociations")
  //      logger.debug(s"extra changes for BCNF tables with no associations: ${uidToViewChanges.filter(cs => cs._2.nChangesInAssociationTables.isEmpty && cs._2.nChangesInBCNFTables.isDefined)
  //      .map(_._2.nChangesInBCNFTables.get).sum}")
  //logger.debug(s"total number of changes in this step: ${nChangesInAssociations+numberOfChangesInTablesWithNoDTTORAssociation}")
  val nChangesInAssociations =GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.sumChangeRanges(
    uidToViewChanges.values.map(_.nChangesInAssociationTables))
  logger.debug(s"number of fields in association set: $nFieldsInAssociations")
  logger.debug(s"number of changes in Associations: $nChangesInAssociations")

  def countChanges(table: TemporalTable) = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(table)

  case class ChangeStats(nChangesInView:(Int,Int),nChangesInBCNFTables:(Int,Int),nChangesInAssociationTables:(Int,Int))

}
