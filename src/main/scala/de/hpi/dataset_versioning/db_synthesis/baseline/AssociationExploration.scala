package de.hpi.dataset_versioning.db_synthesis.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object AssociationExploration extends App with StrictLogging{
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val ids = subDomainInfo(subdomain)
    .map(_.id)
    .toIndexedSeq
  private val existingBCNFTAbles = ids.filter(id => DBSynthesis_IOService.getDecomposedTemporalTableDir(subdomain, id).exists())
  val bcnfTables = existingBCNFTAbles
    .flatMap(id => {
      val associations = DecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain, id)
      associations
    })
  println("pkSize,numBCNFTables")
  bcnfTables.groupBy(a => a.primaryKey.size)
    .map(a => (a._1,a._2.size))
    .toIndexedSeq
    .sortBy(_._1)
    .foreach(t => println(t._1+":"+t._2))
  println("-------------------------------------------------------")
  private val existingAssociations = ids.filter(id => DBSynthesis_IOService.getDecomposedTemporalAssociationDir(subdomain, id).exists())
  logger.debug(s"Extracted associations from ${existingAssociations.size} tables, missing ${ids.size-existingAssociations.size} tables")
  val associations = existingAssociations
    .flatMap(id => {
      val associations = DecomposedTemporalTable.loadAllAssociations(subdomain, id)
      associations
    })
  //stats about association schemata:
  println("pkSize,numAssociations")
  associations.groupBy(a => a.primaryKey.size)
    .map(a => (a._1,a._2.size))
    .toIndexedSeq
    .sortBy(_._1)
    .foreach(t => println(t._1+":"+t._2))
  val intersesting = associations.filter(_.primaryKey.size==7)
  println()
  //associations.foreach(a => SynthesizedDatabaseTable.initFromSingleDecomposedTable())
}
