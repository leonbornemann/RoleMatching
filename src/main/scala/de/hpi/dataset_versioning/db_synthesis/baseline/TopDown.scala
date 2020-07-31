package de.hpi.dataset_versioning.db_synthesis.baseline

import java.time
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.{AttributeState, ChangeCube}
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.TemporalTableDecomposer
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.normalization.DecomposedTable

import scala.collection.mutable
import scala.concurrent.duration.Duration

class TopDown(subdomain:String) extends StrictLogging{



  def synthesizeDatabase() = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    //decompose the tables:
    val temporalTables = subdomainIds.map(id => {
      logger.debug(s"Loading changes for $id")
      val changeCube = ChangeCube.load(id)
      val temporalTable = changeCube.toTemporalTable()
      temporalTable
    })
    val fieldLineageReferences = temporalTables.map(_.getFieldLineageReferences)
    //now we need to build a compatibility index
    val equalityCompatibilityMethod = new FieldLineageEquality()
    val deltaTCompatibilityMethod = new DeltaTCompatibility(3)
//TODO:
    //    temporalTables.foreach(t => {
//      t.validateDiscoverdFDs()
//    })
    //TODO: this might actually be the core of the paper (?)
    //TODO: we need an index and we need to rank the connections
    //TODO: do we aggregate by column?
  }

}
