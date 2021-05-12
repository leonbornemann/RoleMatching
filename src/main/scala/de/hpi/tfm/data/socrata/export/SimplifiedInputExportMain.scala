package de.hpi.tfm.data.socrata.`export`

import de.hpi.tfm.data.socrata.metadata.custom.DatasetInfo
import de.hpi.tfm.io.IOService

import java.io.File

object SimplifiedInputExportMain extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val ids = DatasetInfo.readDatasetInfoBySubDomain(subdomain)
    .map(_.id)
  val identifiedLineageFile = new File(args(2))
  assert(identifiedLineageFile.getParentFile.exists())
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
    val exporter = new SimplifiedInputExporter(subdomain, id)
    exporter.exportAll(identifiedLineageFile)
  })
}
