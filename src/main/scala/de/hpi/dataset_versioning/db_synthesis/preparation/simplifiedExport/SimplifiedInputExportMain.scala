package de.hpi.dataset_versioning.db_synthesis.preparation.simplifiedExport

import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.io.IOService

object SimplifiedInputExportMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val ids = DatasetInfo.readDatasetInfoBySubDomain(subdomain)
    .map(_.id)
  ids.foreach(id => {
//    val cc = ChangeCube.load(id)
//    val ccContainsEvaluationChanges = cc.allChanges.exists(_.t.isAfter(IOService.STANDARD_TIME_FRAME_END))
//    if(ccContainsEvaluationChanges){
//      println(s"cc of $id contains changes that are after ${IOService.STANDARD_TIME_FRAME_END}")
//    }
//    val tt = TemporalTable.from(cc)
//    val allTImstamps = tt.rows.flatMap(r =>
//      r.fields.flatMap(_.lineage.keySet).toSet).toSet
//    val ttContainsEvaluationChanges = allTImstamps.exists(_.isAfter(IOService.STANDARD_TIME_FRAME_END))
//    if(ttContainsEvaluationChanges){
//      println(s"tt of $id contains changes that are after ${IOService.STANDARD_TIME_FRAME_END}")
//    }
//    if(ttContainsEvaluationChanges && !ccContainsEvaluationChanges){
//      println("NOT BOTH!!!!!!!!!!!!!!!!")
//    }
    val exporter = new SimplifiedInputExporter(subdomain,id)
    exporter.exportAll()
  })
}
