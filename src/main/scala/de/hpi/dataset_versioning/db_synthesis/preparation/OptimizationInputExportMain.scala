package de.hpi.dataset_versioning.db_synthesis.preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object OptimizationInputExportMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val id = if (args.length == 3) Some(args(2)) else None
  val exporter = new OptimizationInputExporter(subdomain)
  if (id.isDefined)
    exporter.exportForID(id.get)
  else {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    var subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    val idsToSketch = BCNFTableSchema.filterNotFullyDecomposedTables(subdomain, subdomainIds)
    idsToSketch.foreach(exporter.exportForID(_))
  }

}
