package de.hpi.dataset_versioning.db_synthesis.baseline

import java.io.PrintWriter
import java.time
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, TemporalTableDecomposer}
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable
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
        associations
      })
  }

  def synthesizeDatabase(ids: IndexedSeq[String], countChangesForAllSteps: Boolean):Unit = {
    if (countChangesForAllSteps) {
      val nChangesInViewSet = ids.map(id => TemporalTable.load(id).numChanges.toLong).reduce(_ + _)
      logger.debug(s"number of changes in original view set: $nChangesInViewSet")
    }
    val dtts = loadBCNFDecomposedTables(ids)
    dtts.foreach(t => assert(!t.isAssociation))
    if (countChangesForAllSteps) {
      val nChangesInDecomposedTemporalTables = dtts.map(dtt => SynthesizedTemporalDatabaseTable.initFrom(dtt).numChanges.toLong).reduce(_ + _)
      logger.debug(s"number of changes in normalized temporal tables: $nChangesInDecomposedTemporalTables")
    }
    val temporallyDecomposedAssociations = loadDecomposedAssocations(ids)
    assert(temporallyDecomposedAssociations.size>=dtts.size)
    val nChangesInAssociations = temporallyDecomposedAssociations.map(dtt => SynthesizedTemporalDatabaseTable.initFrom(dtt).numChanges.toLong).reduce(_ + _)
    logger.debug(s"number of changes in decomposed associations: $nChangesInAssociations")
    val topDownOptimizer = new TopDownOptimizer(temporallyDecomposedAssociations,nChangesInAssociations)
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
}
