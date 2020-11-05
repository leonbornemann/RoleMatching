package de.hpi.dataset_versioning.db_synthesis.baseline.decomposition

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.data.simplified.Attribute
import de.hpi.dataset_versioning.db_synthesis.baseline.TopDownMain.args
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object DecomposedTemporalTableCreationMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val id = if(args.size==3) Some(args(2)) else None
  if(!id.isDefined) {
    logger.debug(s"Executing for all ids in $subdomain")
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    createFullyDecomposedTemporalTables(subdomainIds)
  } else{
    val versionHistoryMap = DatasetVersionHistory.load()
      .map(h => (h.id,h))
      .toMap
    processID(versionHistoryMap,id.get)
  }

  def createFullyDecomposedTemporalTables(subdomainIds: IndexedSeq[String]) = {
    val versionHistoryMap = DatasetVersionHistory.load()
      .map(h => (h.id,h))
      .toMap
    val toProcess = subdomainIds
      .filter(id => DBSynthesis_IOService.getDecomposedTableFile(subdomain,id,versionHistoryMap(id).latestChangeTimestamp).exists())
    println()
    toProcess.foreach(id => processID(versionHistoryMap, id))
  }

  private def processID(versionHistoryMap: Map[String, DatasetVersionHistory], id: String) = {
    logger.debug(s"decomposing table $id")
    val versionHistory = versionHistoryMap(id)
    val decomposer = new TemporalTableDecomposer(subdomain, id, versionHistory)
    decomposer.createDecomposedTemporalTables()
  }
}
