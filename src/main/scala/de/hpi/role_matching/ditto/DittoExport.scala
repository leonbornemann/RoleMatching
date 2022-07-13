package de.hpi.role_matching.ditto

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.blocking.TransitionSetBlocking
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source
import scala.sys.process._

object DittoExport extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  val datasource = args(0)
  val rolesetDir = args(1)
  val trainTimeEnd = LocalDate.parse(args(2))
  val resultRootDir = new File(args(3))
  val exportEntityPropertyIDs = args(4).toBoolean
  val exportSampleOnly = args(5).toBoolean
  val dsNames = args(6).split(",")
  val maxSampleSize = args(7).toInt
  val inputCandidateFile = if(args.size==9) Some(args(8)) else None
  logger.debug("Running ",rolesetDir)
  GLOBAL_CONFIG.setSettingsForDataSource(datasource)
  val rolesetFiles = new File(rolesetDir).listFiles()
  for(rolesetFile <- rolesetFiles){
    val dsName = rolesetFile.getName.split("\\.")(0)
    if(dsNames.contains(dsName)) {
      logger.debug("Running {}", rolesetFile)
      val resultFile = new File(s"$resultRootDir/${rolesetFile.getName}.txt")
      val vertices = Roleset.fromJsonFile(rolesetFile.getAbsolutePath)
      val blocker = new TransitionSetBlocking(vertices, trainTimeEnd)
      val exporter = new DittoExporter(vertices, trainTimeEnd, Some(blocker), resultFile, exportEntityPropertyIDs, false, exportSampleOnly, maxSampleSize)
      if(inputCandidateFile.isEmpty)
        exporter.exportDataWithSimpleBlocking()
      else
        exporter.exportDataForMatchFile(SimpleCompatbilityGraphEdgeID.iterableFromJsonObjectPerLineFile(inputCandidateFile.get))
    } else {
      logger.debug(s"Skipping $dsName")
    }
  }
}

