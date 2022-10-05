package de.hpi.role_matching.evaluation.blocking.missing_values

import de.hpi.role_matching.data.Roleset
import de.hpi.role_matching.evaluation.blocking.ground_truth.DatasetAndIDJson
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate
import scala.util.Random

//de.hpi.role_matching.data.Roleset.MissingDataRolesetGenerator /san2/data/change-exploration/roleMerging/finalExperiments/finalRolesets/ /san2/data/change-exploration/roleMerging/finalExperiments/rolesetsWithSyntheticMissingData/
object MissingDataRolesetGenerator extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val rolesetFiles = new File(args(0)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultDir = new File(args(1))

  def readIDsInGoldStandardFromFiles(dfgsPath: String, rgsPath: String) = {
    val dgs = DatasetAndIDJson.fromJsonObjectPerLineFile(dfgsPath)
      .groupBy(_.dataset)
      .map(s => (s._1, s._2.flatMap(t => Seq(t.id1, t.id2)).toSet))
    val rgs = DatasetAndIDJson.fromJsonObjectPerLineFile(rgsPath)
      .groupBy(_.dataset)
      .map(s => (s._1, s._2.flatMap(t => Seq(t.id1, t.id2)).toSet))
    (dgs, rgs)
  }

  val (dgsIds, rgsIds) = readIDsInGoldStandardFromFiles("/home/leon/data/dataset_versioning/finalExperiments/idInGoldStandards.csv", "/home/leon/data/dataset_versioning/finalExperiments/idInGoldStandards2.csv")
  resultDir.mkdirs()
  val missingDataThresholds = IndexedSeq(0.1, 0.2, 0.3, 0.4, 0.5)
  val trainTimeEnd = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
  missingDataThresholds.foreach(d => {
    println(d)
    val resultDirThis = new File(resultDir.getAbsolutePath + s"/$d/")
    resultDirThis.mkdirs()
    rolesetFiles.foreach(f => {
      println(f)
      val random = new Random(13)
      val rs = Roleset.fromJsonFile(f.getAbsolutePath)
      new MissingDataTransformer(rs, random, trainTimeEnd, dgsIds(f.getName.split("\\.")(0)), rgsIds(f.getName.split("\\.")(0)))
        .getTransformedRoleFile(d)
        .toJsonFile(new File(resultDirThis.getAbsolutePath + "/" + f.getName))
    })
  })
}
