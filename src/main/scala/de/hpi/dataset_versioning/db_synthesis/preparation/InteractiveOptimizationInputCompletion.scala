package de.hpi.dataset_versioning.db_synthesis.preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.io.InteractiveSaveDeleteMain.logger

object InteractiveOptimizationInputCompletion extends App with StrictLogging {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val ids = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  val exporter = new OptimizationInputExporter(subdomain)
  val toComplete = ids.filter(id => {
      val associations = AssociationSchema.loadAllAssociations(subdomain, id)
      val res = associations.exists(a => {
        !SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch.getStandardOptimizationInputFile(a.id).exists() ||
          !SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.getStandardOptimizationInputFile(a.id).exists()
      })
      res
    })
  logger.debug(s"Found the following ids to complete: $toComplete")
  logger.debug("Type in y to continue, anything else to quit")
  val continue = scala.io.StdIn.readLine()
  if(continue.toLowerCase=="y"){
    toComplete.foreach(id => {
      exporter.exportForID(id)
    })
  } else{
    logger.debug("terminating")
  }
}
