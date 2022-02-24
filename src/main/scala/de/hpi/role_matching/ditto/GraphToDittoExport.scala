package de.hpi.role_matching.ditto

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.{SimpleCompatbilityGraphEdge, SimpleCompatbilityGraphEdgeID}
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object GraphToDittoExport extends App with StrictLogging {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val simpleEdgeDir = new File(args(1))
  val rolesetDir = new File(args(2))
  val trainTimeEnd = LocalDate.parse(args(3))
  val resultDir = args(4)
  for (dir <- simpleEdgeDir.listFiles()){
    logger.debug(s"Processing $dir")
    val dsName = dir.getName
    val rolesetFile = rolesetDir.getAbsolutePath + s"/$dsName.json"
    val roleset = Roleset.fromJsonFile(rolesetFile)
    val resultFile = new File(s"$resultDir/$dsName.txt")
    val exporter = new DittoExporter(roleset,trainTimeEnd,resultFile)
    exporter.exportDataForGraph(SimpleCompatbilityGraphEdge.iterableFromEdgeIDObjectPerLineDir(dir,roleset))

  }


}
