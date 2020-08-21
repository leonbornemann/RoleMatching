package de.hpi.dataset_versioning.db_synthesis.sketches

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.top_down_no_change.decomposition.DatasetInfo
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object ColumnSketchExportMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val id = if(args.length==3) Some(args(2)) else None

  def exportForID(id: String) = {
    logger.debug(s"Exporting $id")
    //individual columns
    val tt = TemporalTable.load(id)
    val tcs = tt.getTemporalColumns()
    tcs.foreach(tc => {
      val sketch = TemporalColumnSketch.from(tc)
      val f = DBSynthesis_IOService.getTemporalColumnSketchFile(tc.id,tc.attrId,sketch.fieldLineageSketches.head.getVariantName)
      sketch.writeToBinaryFile(f)
    })
//    val dtts = DecomposedTemporalTable.loadAllAssociations(subdomain,id)
//    dtts.foreach(dtt => {
//      val projection = tt.project(dtt)
//      val tcs = projection.getTemporalColumns()
//      val firstEntityIds = tcs.head.lineages.map(_.entityID)
//      assert(tcs.forall(tc => tc.lineages.map(_.entityID)==firstEntityIds))
//      val dttSketch = new DecomposedTemporalTableSketch(dtt.id,tcs.map(tc => TemporalColumnSketch.from(tc)).toArray)
//      dttSketch.writeToStandardFile()
//    })
  }

  if(id.isDefined)
    exportForID(id.get)
  else {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
      .toIndexedSeq
    subdomainIds.foreach(exportForID(_))
  }

}
