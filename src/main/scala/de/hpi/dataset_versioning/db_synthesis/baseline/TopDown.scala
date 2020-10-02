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
import de.hpi.dataset_versioning.db_synthesis.baseline.database.SynthesizedTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTable, DecomposedTemporalTable, TemporalTableDecomposer}
import de.hpi.dataset_versioning.db_synthesis.database.query_tracking.ViewQueryTracker
import de.hpi.dataset_versioning.db_synthesis.sketches.field.Variant2Sketch
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable
import scala.concurrent.duration.Duration

class TopDown(subdomain:String) extends StrictLogging{

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
    val (idsWithDecomposedTables,idsWithFailedDecomposition) = ids.partition(id => DBSynthesis_IOService.getDecomposedTemporalTableDir(subdomain, id).exists())
    val allAssociations:mutable.ArrayBuffer[DecomposedTemporalTable] = mutable.ArrayBuffer()
    val extraNonDecomposedViewTableChanges = mutable.HashMap[String,Long]()
    val extraBCNFDtts = mutable.HashSet[DecomposedTemporalTable]()
    ids.foreach(id => {
      var associations:Array[DecomposedTemporalTable] = null
      var tt:TemporalTable = null
      if(DBSynthesis_IOService.decomposedTemporalAssociationsExist(subdomain,id)) {
        associations = DecomposedTemporalTable.loadAllAssociations(subdomain, id)
        allAssociations ++= associations
        if(tt==null)
          tt = TemporalTable.load(id)
        writeSketchesIfNotPresent(associations,tt)
        //TODO: check if there are dtts that have no associations
        val allDtts = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
        val associationByBCNFID = associations.groupBy(_.id.bcnfID)
        allDtts.foreach(dtt => {
          if(!associationByBCNFID.contains(dtt.id.bcnfID)){
            extraBCNFDtts.add(dtt)
          }
        })
      } else if(!DBSynthesis_IOService.decomposedTemporalTablesExist(subdomain,id)){
        if(tt==null)
          tt = TemporalTable.load(id)
        extraNonDecomposedViewTableChanges.put(id,tt.numChanges)
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
          bcnfChangeCount = Some(dtts.map(dtt => SynthesizedTemporalDatabaseTable.initFrom(dtt, tt).numChanges.toLong).reduce(_ + _))
        }
        if(!DBSynthesis_IOService.decomposedTemporalAssociationsExist(subdomain, id)) {
          logger.debug(s"no decomposed Temporal associations found for $id, skipping this")
        } else{
          associationChangeCount = Some(associations.map(a => SynthesizedTemporalDatabaseTable.initFrom(a, tt).numChanges.toLong).reduce(_ + _))
        }
        uidToViewChanges.put(id, ChangeStats(tt.numChanges, bcnfChangeCount, associationChangeCount))
      }
    })
    if (countChangesForAllSteps) {
      val nChangesInViewSet = uidToViewChanges.values.map(_.nChangesInView).reduce(_ + _)
      logger.debug(s"number of changes in original view set: $nChangesInViewSet")
      val nChangesInBCNFTables = uidToViewChanges.values.filter(_.nChangesInBCNFTables.isDefined).map(_.nChangesInBCNFTables.get).reduce(_ + _)
      logger.debug(s"number of changes in BCNF tables: $nChangesInBCNFTables")
      logger.debug(s"total number of changes in this step: ${nChangesInBCNFTables+extraNonDecomposedViewTableChanges.values.sum}")
      val nChangesInAssociations = uidToViewChanges.values.filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get).reduce(_ + _)
      logger.debug(s"number of changes in associations: $nChangesInAssociations")
      //logger.debug(s"total number of changes in this step: ${nChangesInAssociations+numberOfChangesInTablesWithNoDTTORAssociation}")
    }
    val queryTracker = new ViewQueryTracker(idsWithDecomposedTables)
    assert(countChangesForAllSteps)
    val nChangesInAssociations = uidToViewChanges.values.filter(_.nChangesInAssociationTables.isDefined).map(_.nChangesInAssociationTables.get).reduce(_ + _)
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
