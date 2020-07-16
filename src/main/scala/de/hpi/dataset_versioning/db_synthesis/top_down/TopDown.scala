package de.hpi.dataset_versioning.db_synthesis.top_down

import java.time
import java.time.temporal.ChronoUnit

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.DatasetInfo

import scala.concurrent.duration.Duration

class TopDown(subdomain:String) extends StrictLogging{

  def synthesizeDatabase() = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
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
