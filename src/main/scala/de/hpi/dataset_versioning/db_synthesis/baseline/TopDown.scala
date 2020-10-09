package de.hpi.dataset_versioning.db_synthesis.baseline

import java.io.PrintWriter
import java.time
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.config.{GLOBAL_CONFIG, InitialInsertIgnoreFieldChangeCounter}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.SynthesizedTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTable, DecomposedTemporalTable, TemporalTableDecomposer}
import de.hpi.dataset_versioning.db_synthesis.database.query_tracking.ViewQueryTracker
import de.hpi.dataset_versioning.db_synthesis.sketches.field.Variant2Sketch
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable
import scala.concurrent.duration.Duration

class TopDown(subdomain:String,idsToIgnore:Set[String]=Set()) extends StrictLogging{

  val changeCounters = Seq(new InitialInsertIgnoreFieldChangeCounter())

  def synthesizeDatabase(countChangesForAllSteps:Boolean = true):Unit = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    synthesizeDatabase(subdomainIds,countChangesForAllSteps)
  }

  def loadDecomposedAssocations(ids: IndexedSeq[String]) = {
    ids.filter(id => DBSynthesis_IOService.getDecomposedTemporalAssociationDir(subdomain, id).exists())
      .flatMap(id => {
        logger.debug(s"Loading temporally decomposed tables for $id")
        val associations = DecomposedTemporalTable.loadAllAssociations(subdomain, id)
        val dtts = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
        associations
      })
  }

  def writeSketchesIfNotPresent(associations: Array[DecomposedTemporalTable], tt: TemporalTable) = {
    associations.foreach(a => {
      if(!DBSynthesis_IOService.getDecomposedTemporalTableSketchFile(a.id,Variant2Sketch.getVariantName).exists())
        tt.project(a).projection.writeTableSketch(a.primaryKey.map(_.attrId))
    })
    //.foreach{case (tt,dtt) =>   tt.project(dtt).projection.writeTableSketch(dtt.primaryKey.map(_.attrId))
  }

  def synthesizeDatabase(ids: IndexedSeq[String], countChangesForAllSteps: Boolean):Unit = {
    val uidToViewChanges:mutable.HashMap[String,ChangeStats] = mutable.HashMap()
    val idsWithDecomposedTables = DecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain,ids)
      .filter(!idsToIgnore.contains(_))
    if(countChangesForAllSteps){
      val nonDecomposed = ids.diff(idsWithDecomposedTables)
      var nChanges:Long = 0
      nonDecomposed.foreach(id => {
        nChanges += TemporalTable.load(id).countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD,Set())
      })
      logger.debug(s"nCHanges of non-decomposed tables: $nChanges")
    }
    val allAssociations:mutable.ArrayBuffer[DecomposedTemporalTable] = mutable.ArrayBuffer()
    val extraNonDecomposedViewTableChanges = mutable.HashMap[String,Long]()
    val extraBCNFDtts = mutable.HashSet[DecomposedTemporalTable]()
    idsWithDecomposedTables.foreach(id => {
      var associations:Array[DecomposedTemporalTable] = null
      var tt:TemporalTable = null
      if(DBSynthesis_IOService.decomposedTemporalAssociationsExist(subdomain,id)) {
        associations = DecomposedTemporalTable.loadAllAssociations(subdomain, id)
        allAssociations ++= associations
        //write sketches if not present:
        associations.foreach(a => {
          if(!DBSynthesis_IOService.getDecomposedTemporalTableSketchFile(a.id,Variant2Sketch.getVariantName).exists()) {
            if(tt==null)
              tt = TemporalTable.load(id)
            tt.project(a).projection.writeTableSketch(a.primaryKey.map(_.attrId))
          }
        })
        val allDtts = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
        val associationByBCNFID = associations.groupBy(_.id.bcnfID)
        allDtts.foreach(dtt => {
          if(!associationByBCNFID.contains(dtt.id.bcnfID)){
            extraBCNFDtts.add(dtt)
          }
        })
      } else if(!DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain,id) && countChangesForAllSteps){
        if(tt==null)
          tt = TemporalTable.load(id)
        extraNonDecomposedViewTableChanges.put(id,tt.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD,Set()))
      }
      if(countChangesForAllSteps) {
        if(tt==null)
          tt = TemporalTable.load(id)
        var bcnfChangeCount: Option[Long] = None
        var associationChangeCount: Option[Long] = None
        if (!DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain, id)) {
          logger.debug(s"no decomposed Temporal tables found for $id, skipping this")
        } else {
          val dtts = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
          bcnfChangeCount = Some(dtts.map(dtt => SynthesizedTemporalDatabaseTable.initFrom(dtt, tt).countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)).reduce(_ + _))
        }
        if(!DBSynthesis_IOService.decomposedTemporalAssociationsExist(subdomain, id)) {
          logger.debug(s"no decomposed Temporal associations found for $id, skipping this")
        } else{
          associationChangeCount = Some(associations.map(a => SynthesizedTemporalDatabaseTable.initFrom(a, tt).countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)).reduce(_ + _))
        }
        uidToViewChanges.put(id, ChangeStats(tt.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD,Set()), bcnfChangeCount, associationChangeCount))
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
    val queryTracker = new ViewQueryTracker(idsWithDecomposedTables)
    val nChangesInAssociations = if(countChangesForAllSteps) uidToViewChanges.values.filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get).reduce(_ + _) else -1
    if(!countChangesForAllSteps)
      logger.debug("Not counting all changes, thus initializing topdown optimizer with a dummy initial change value")
    val topDownOptimizer = new TopDownOptimizer(allAssociations.toIndexedSeq,
      nChangesInAssociations,
      extraBCNFDtts.toSet,
      extraNonDecomposedViewTableChanges.toMap,
      Some(queryTracker))
    topDownOptimizer.optimize()
  }

  private def loadBCNFDecomposedTables(ids: IndexedSeq[String]) = {
    ids.filter(id => DBSynthesis_IOService.getDecomposedTemporalTableDir(subdomain, id).exists())
      .flatMap(id => {
        logger.debug(s"Loading temporally decomposed tables for $id")
        val dtts = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
        dtts
      })
  }

  case class ChangeStats(nChangesInView:Long,nChangesInBCNFTables:Option[Long],nChangesInAssociationTables:Option[Long])

}
object TopDown extends StrictLogging{
  logger.debug("Currently passing empty set as primary key to temporal tables!")
}
