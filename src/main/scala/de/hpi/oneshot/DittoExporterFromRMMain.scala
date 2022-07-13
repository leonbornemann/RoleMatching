package de.hpi.oneshot

import de.hpi.role_matching.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object DittoExporterFromRMMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDir = new File(args(0))
  val outputDir = new File(args(1))
  val tmpOutputDir = new File(args(2))
  val trainTimeEnd = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
  val rolesetDir = args(3)
  val exportEntityPropertyIDs = args(4).toBoolean
  val exportSampleOnly = args(5).toBoolean
  val maxSampleSize = args(6).toInt
  val runSamplePreparation = args(7).toBoolean
  val toExclude = args(8).split(",")
  inputDir.listFiles().foreach(f => {
    println("processing",f)
    if(!toExclude.contains(s"${f.getName}")){
      val outputFile = new File(outputDir.getAbsolutePath + s"/${f.getName}.json.txt")
      val rolesetFile = rolesetDir + s"/${f.getName}.json"
      val expoeter = new DittoExporterFromRM(f,outputFile,tmpOutputDir,rolesetFile,trainTimeEnd,exportEntityPropertyIDs,exportSampleOnly,maxSampleSize)
      if(runSamplePreparation)
        expoeter.prepareSample()
      expoeter.exportSample()
    }
  })


}
