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
      val exportedLines = exporter.exportData()
      val filename = resultFile.getAbsolutePath
      //shuffle file
      val cmd = s"shuf $filename -o $filename" // Your command
      val output = cmd.!
      println(s"Output of shuffle command: $output")
      //train / validation / test split
      val trainFile = new PrintWriter(s"${filename}_train.txt")
      val validationFile = new PrintWriter(s"${filename}_validation.txt")
      val testFile = new PrintWriter(s"${filename}_test.txt")
      val validationLineBegin = (exportedLines*0.6).toInt
      val testLineBegin = (exportedLines*0.8).toInt
      var curCount = 0
      Source.fromFile(resultFile).getLines().foreach{s =>
        if(curCount<validationLineBegin)
          trainFile.println(s)
        else if (curCount<testLineBegin)
          validationFile.println(s)
        else
          testFile.println(s)
        curCount+=1
      }
      trainFile.close()
      validationFile.close()
      testFile.close()
    }
  }
}

