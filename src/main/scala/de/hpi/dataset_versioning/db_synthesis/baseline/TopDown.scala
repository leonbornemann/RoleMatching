package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.change_counting.natural_key_based.DatasetInsertIgnoreFieldChangeCounter
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable

class TopDown(subdomain:String,idsToIgnore:Set[String]=Set()) extends StrictLogging{

  val changeCounters = Seq(new DatasetInsertIgnoreFieldChangeCounter())

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
    val idsWithDecomposedTables = BCNFTableSchema.filterNotFullyDecomposedTables(subdomain,ids)
      .filter(!idsToIgnore.contains(_))
    logger.debug(s"Running Database synthesis for ${idsWithDecomposedTables.size} ids: $idsWithDecomposedTables")
    if(countChangesForAllSteps){
      val nonDecomposed = ids.diff(idsWithDecomposedTables)
      var nChanges:Long = 0
      nonDecomposed.foreach(id => {
        nChanges += countChanges(TemporalTable.load(id))
      })
      logger.debug(s"nCHanges of non-decomposed tables: $nChanges")
    }
    val allAssociations:mutable.ArrayBuffer[AssociationSchema] = mutable.ArrayBuffer()
    val extraNonDecomposedViewTableChanges = mutable.HashMap[String,Long]()
    idsWithDecomposedTables.foreach(id => {
      var associations:Array[AssociationSchema] = null
      var tt:TemporalTable = null
      if(DBSynthesis_IOService.associationSchemataExist(subdomain,id)) {
        associations = AssociationSchema.loadAllAssociations(subdomain, id)
        allAssociations ++= associations
        //write sketches if not present:
        val missing = associations.filter(a => !DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(a.id).exists())
//        missing.foreach(a => println(s"${a.id}  $a"))
//        if(id=="72qm-3bwf"){
//          println("ahaa")
//          println(missing.size)
//          associations.foreach(println(_))
//
//        }
        associations.foreach(a => assert(DBSynthesis_IOService.getOptimizationInputAssociationSketchFile(a.id).exists()))
      } else if(!DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain,id) && countChangesForAllSteps){
        if(tt==null)
          tt = TemporalTable.load(id)
        extraNonDecomposedViewTableChanges.put(id,countChanges(tt))
      }
      if(countChangesForAllSteps) {
        if(tt==null)
          tt = TemporalTable.load(id)
        var bcnfChangeCount: Option[Long] = None
        var associationChangeCount: Option[Long] = None
        if (!DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain, id)) {
          logger.debug(s"no decomposed Temporal tables found for $id, skipping this")
        } else {
          val dtts = SurrogateBasedDecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
          bcnfChangeCount = Some(dtts.map(dtt => GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(TemporalTable.loadBCNFFromStandardBinaryFile(dtt.id))).sum)
        }
        if(!DBSynthesis_IOService.associationSchemataExist(subdomain, id)) {
          logger.debug(s"no decomposed Temporal associations found for $id, skipping this")
        } else{
          associationChangeCount = Some(associations.map(a => GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(a.id))).sum)
        }
        uidToViewChanges.put(id, ChangeStats(countChanges(tt), bcnfChangeCount, associationChangeCount))
      }
    })
    if (countChangesForAllSteps) {
      logger.debug(s"extra Non-decomposed temporal table changes (should be zero): ${extraNonDecomposedViewTableChanges.values.sum}")
      val nChangesInViewSet = uidToViewChanges.values.map(_.nChangesInView).reduce(_ + _)
      logger.debug(s"number of changes in view set, where normalization result exists: $nChangesInViewSet")
      val nChangesInBCNFTables = uidToViewChanges.values.filter(_.nChangesInBCNFTables.isDefined).map(_.nChangesInBCNFTables.get).reduce(_ + _)
      logger.debug(s"number of changes in BCNF tables: $nChangesInBCNFTables")
      logger.debug(s"total number of changes in this step: ${nChangesInBCNFTables+extraNonDecomposedViewTableChanges.values.sum}")
      val nChangesInAssociations = uidToViewChanges.values.filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get).reduce(_ + _)
      logger.debug(s"number of changes in associations: $nChangesInAssociations")
      logger.debug(s"extra changes for BCNF tables with no associations: ${uidToViewChanges.filter(cs => cs._2.nChangesInAssociationTables.isEmpty && cs._2.nChangesInBCNFTables.isDefined)
      .map(_._2.nChangesInBCNFTables.get).sum}")
      //logger.debug(s"total number of changes in this step: ${nChangesInAssociations+numberOfChangesInTablesWithNoDTTORAssociation}")
    }
    val nChangesInAssociations = if(countChangesForAllSteps) uidToViewChanges.values.filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get).reduce(_ + _) else -1
    if(!countChangesForAllSteps) {
      logger.debug("Not counting all changes, thus initializing topdown optimizer with a dummy initial change value")
    }
    //TODO: use this:
    val bcnfSchemata = idsWithDecomposedTables.flatMap(id => BCNFTableSchema.loadAllBCNFTableSchemata(subdomain,id))
    val topDownOptimizer = new TopDownOptimizer(allAssociations.toIndexedSeq,
      bcnfSchemata,
      nChangesInAssociations,
      extraNonDecomposedViewTableChanges.toMap)
    topDownOptimizer.optimize()
  }

  case class ChangeStats(nChangesInView:Long,nChangesInBCNFTables:Option[Long],nChangesInAssociationTables:Option[Long])

}
object TopDown extends StrictLogging{
  logger.debug("Currently passing empty set as primary key to temporal tables!")
}
