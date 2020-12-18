package de.hpi.dataset_versioning.db_synthesis.sketches.column

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.table.{AssociationSchema, BCNFTableSchema}
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object OptimizationInputExportMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val id = if (args.length == 3) Some(args(2)) else None

  def exportForID(id: String) = {
    logger.debug(s"Exporting $id")
    //individual columns
    val tt = TemporalTable.load(id)
//    val tcs = tt.getTemporalColumns()
//    tcs.foreach(tc => {
//      val sketch = TemporalColumnSketch.from(tc)
//      val f = DBSynthesis_IOService.getTemporalColumnSketchFile(tc.id, tc.attrId, sketch.fieldLineageSketches.head.getVariantName)
//      sketch.writeToBinaryFile(f)
//    })
    //bcnf Tables:

    val bcnfTables = BCNFTableSchema.loadAllBCNFTableSchemata(subdomain,id)
    val bcnfByID = bcnfTables.map(a => ((a.id.subdomain,a.id.viewID,a.id.bcnfID),a)).toMap
    //whole tables:
    val associations = if(DBSynthesis_IOService.associationSchemataExist(subdomain,id)) AssociationSchema.loadAllAssociations(subdomain, id) else Array[AssociationSchema]()
    val allSurrogates = bcnfTables.flatMap(_.surrogateKey)
      .groupBy(_.surrogateID)
      .map{case (k,v) => (k,v.head)}
    //integrity check: all references must also be used as keys:
    assert(bcnfTables.flatMap(_.foreignSurrogateKeysToReferencedBCNFTables.toMap.keySet).forall(s => allSurrogates.contains(s.surrogateID)))
    tt.addSurrogates(allSurrogates.values.toSet  ++ associations.map(_.surrogateKey).toSet)
    //for change counting purposes we write the projections of bcnf tables containing data:
    val dtts = SurrogateBasedDecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
        dtts.foreach(dtt =>{
      val projection = tt.project(dtt)
      projection.projection.writeTOBCNFTemporalTableFile
    })
    val byBcnf = associations.groupBy(a => (a.id.subdomain,a.id.viewID,a.id.bcnfID))
      .map{case (k,v) => (bcnfByID(k),v)}
    byBcnf.foreach{case (bcnf,associations) => {
      val (bcnfReferenceTable,synthTableAssocitations) = tt.project(bcnf,associations)
      bcnfReferenceTable.writeToStandardOptimizationInputFile
      synthTableAssocitations.foreach(t => {
        t.writeToStandardOptimizationInputFile
        t.toSketch.writeToStandardOptimizationInputFile()
      })
    }}
    //additionally serialize bcnf tables that have only keys and foreign keys (and no associations, so they don't show up in byBcnf):
    bcnfTables.toSet.diff(byBcnf.keySet).foreach(bcnf => {
      val (bcnfReferenceTable,_) = tt.project(bcnf,Seq())
      bcnfReferenceTable.writeToStandardOptimizationInputFile
    })
  }

  if (id.isDefined)
    exportForID(id.get)
  else {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    var subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    val idsToSketch = BCNFTableSchema.filterNotFullyDecomposedTables(subdomain,subdomainIds)
    idsToSketch.foreach(exportForID(_))
  }

}
