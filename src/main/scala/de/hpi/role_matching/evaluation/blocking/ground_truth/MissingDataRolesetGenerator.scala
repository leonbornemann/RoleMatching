package de.hpi.role_matching.evaluation.blocking.ground_truth

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.util.Random
//de.hpi.role_matching.data.Roleset.MissingDataRolesetGenerator /san2/data/change-exploration/roleMerging/finalExperiments/finalRolesets/ /san2/data/change-exploration/roleMerging/finalExperiments/rolesetsWithSyntheticMissingData/
object MissingDataRolesetGenerator extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val rolesetFiles = new File(args(0)).listFiles()
  //val rolesets = rolesetFiles.map(f => Roleset.fromJsonFile(f.getAbsolutePath))
  val resultDir = new File(args(1))
  resultDir.mkdirs()
  val missingDataThresholds = IndexedSeq(0.5,0.6,0.7,0.8)
  val trainTimeEnd = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
  missingDataThresholds.foreach(d => {
    println(d)
    val resultDirThis = new File(resultDir.getAbsolutePath + s"/$d/")
    resultDirThis.mkdirs()
    rolesetFiles.foreach(f => {
      println(f)
      val random = new Random(13)
      val rs = Roleset.fromJsonFile(f.getAbsolutePath,Some(d),Some(trainTimeEnd),Some(random))
      rs.toJsonFile(new File(resultDirThis.getAbsolutePath + f.getName))
    })
  })
}
