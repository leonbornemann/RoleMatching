package de.hpi.dataset_versioning.db_synthesis.sketches.column

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, SurrogateBasedDecomposedTemporalTable}
import de.hpi.dataset_versioning.db_synthesis.sketches.table.DecomposedTemporalTableSketch
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object ColumnSketchExportMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val id = if (args.length == 3) Some(args(2)) else None

  def exportForID(id: String) = {
    logger.debug(s"Exporting $id")
    //individual columns
    val tt = TemporalTable.load(id)
    val tcs = tt.getTemporalColumns()
    tcs.foreach(tc => {
      val sketch = TemporalColumnSketch.from(tc)
      val f = DBSynthesis_IOService.getTemporalColumnSketchFile(tc.id, tc.attrId, sketch.fieldLineageSketches.head.getVariantName)
      sketch.writeToBinaryFile(f)
    })
    //bcnf Tables:
    val bcnfTables = SurrogateBasedDecomposedTemporalTable.loadAllDecomposedTemporalTables(subdomain,id)
    //whole tables:
    val associations = if(DBSynthesis_IOService.decomposedTemporalAssociationsExist(subdomain,id)) SurrogateBasedDecomposedTemporalTable.loadAllAssociations(subdomain, id) else Array[SurrogateBasedDecomposedTemporalTable]()
    val allSurrogates = bcnfTables.flatMap(_.surrogateKey)
      .groupBy(_.surrogateID)
      .map{case (k,v) => (k,v.head)}
    //integrity check: all references must also be used as keys:
    assert(bcnfTables.flatMap(_.foreignSurrogateKeysToReferencedTables.toMap.keySet).forall(s => allSurrogates.contains(s.surrogateID)))

    tt.addSurrogates(allSurrogates.values.toSet)
    val dtts = bcnfTables ++ associations
    dtts.foreach(dtt => {
      val projection = tt.project(dtt)
      projection.projection.writeTableSketch
    })
  }

  if (id.isDefined)
    exportForID(id.get)
  else {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    val idsToSketch = SurrogateBasedDecomposedTemporalTable.filterNotFullyDecomposedTables(subdomain,subdomainIds)
    idsToSketch.foreach(exportForID(_))
  }

}
