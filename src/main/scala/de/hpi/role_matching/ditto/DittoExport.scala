package de.hpi.role_matching.ditto

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source
import scala.sys.process._

object DittoExport extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  val datasources = args(0).split(";")
  val rolesetDirs = args(1).split(";")
  val trainTimeEnds = args(2).split(";").map(LocalDate.parse(_))
  val resultRootDir = new File(args(3))
  assert(rolesetDirs.size==datasources.size)
  for(((source,rolesetDir),trainTimeEnd) <- datasources.zip(rolesetDirs).zip(trainTimeEnds)){
    logger.debug("Running ",source,rolesetDir)
    GLOBAL_CONFIG.setSettingsForDataSource(source)
    val rolesetFiles = new File(rolesetDir).listFiles()
    for(rolesetFile <- rolesetFiles){
      logger.debug("Running {}",rolesetFile)
      val resultDir = new File(resultRootDir.getAbsolutePath + s"/${new File(rolesetDir).getName}")
      val resultFile = new File(s"${resultDir.getAbsolutePath}/${rolesetFile.getName}.txt")
      resultDir.mkdir()
      val vertices = Roleset.fromJsonFile(rolesetFile.getAbsolutePath)
      val exporter = new DittoExporter(vertices,trainTimeEnd,resultFile)
      exporter.exportDataWithSimpleBlocking()
    }
  }
}

