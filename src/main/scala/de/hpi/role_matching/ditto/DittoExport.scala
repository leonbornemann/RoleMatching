package de.hpi.role_matching.ditto

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object DittoExport extends App with StrictLogging{
  println(s"Called with ${args.toIndexedSeq}")
  val datasources = args(0).split(";")
  val rolesetDirs = args(1).split(";")
  val trainTimeEnd = LocalDate.parse(args(2))
  val resultRootDir = new File(args(3))
  assert(rolesetDirs.size==datasources.size)
  for((source,rolesetDir) <- datasources.zip(rolesetDirs)){
    logger.debug("Running ",source,rolesetDir)
    GLOBAL_CONFIG.setSettingsForDataSource(source)
    val rolesetFiles = new File(rolesetDir).listFiles()
    for(rolesetFile <- rolesetFiles){
      logger.debug("Running {}",rolesetFile)
      val resultFile = new File(resultRootDir.getAbsolutePath + s"/${new File(rolesetDir).getName}/${rolesetFile.getName}.txt")
      val vertices = Roleset.fromJsonFile(rolesetFile.getAbsolutePath)
      val exporter = new DittoExporter(vertices,trainTimeEnd,resultFile)
      exporter.exportData()
    }
  }

}
