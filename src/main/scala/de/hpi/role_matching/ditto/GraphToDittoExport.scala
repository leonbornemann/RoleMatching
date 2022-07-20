package de.hpi.role_matching.ditto

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object GraphToDittoExport extends App with StrictLogging {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val configDir = new File(args(1))
  val rolesetDir = new File(args(2))
  val trainTimeEnd = LocalDate.parse(args(3))
  val resultDir = args(4)
  val exportIDs = args(5).toBoolean
  val exportEvidenceCounts = args(6).toBoolean
  new File(resultDir).mkdirs()
  for (datasetDir <- configDir.listFiles()){
    val dsName = datasetDir.getName
    logger.debug(s"Processing $datasetDir")
    logger.debug(s"Processing $dsName")
    val graphDir = new File(datasetDir.getAbsolutePath + "/edges/")
    val rolesetFile = rolesetDir.getAbsolutePath + s"/$dsName.json"
    val roleset = Roleset.fromJsonFile(rolesetFile)
    val resultFile = new File(s"$resultDir/$dsName.txt")
    val exporter = new DittoExporter(roleset,trainTimeEnd,None,resultFile,exportIDs,exportEvidenceCounts,false)
    exporter.exportDataForGraph(SimpleCompatbilityGraphEdge.iterableFromEdgeIDObjectPerLineDir(graphDir,roleset))
  }
}
