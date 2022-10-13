package de.hpi.role_matching.matching

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

object FeatureTableExport extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val inputDir = new File("/home/leon/data/dataset_versioning/finalExperiments/MatchingTrainTestData")
  val outputDir = new File("/home/leon/data/dataset_versioning/finalExperiments/MatchingTrainTestDataFeatures/")
  val rolesetDir = new File("/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/")
  val trainTimeEnd = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
  outputDir.mkdirs()
  inputDir
    .listFiles()
    .foreach(f => {
      println(f)
      val ds = f.getName
      val roleset = Roleset.fromJsonFile(rolesetDir.getAbsolutePath + "/" + ds + ".json")
      val thisOutputDir = new File(outputDir.getAbsolutePath + "/" + ds)
      val featureTableExporter = new FeatureTableExporter(f,roleset,thisOutputDir,trainTimeEnd)
      featureTableExporter.exportFeaturesForAll()
    })

}
