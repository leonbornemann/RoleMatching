package de.hpi.wikipedia_data_preparation

import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleLineageWithID, Roleset}
import scala.collection.parallel.CollectionConverters._



import java.io.File
import scala.io.Source

object TemplateDirToRolesetMain extends App {
  val templateRootDir = new File(args(0))
  val rolesetRootDir = new File(args(1))
  val datasetList = Source.fromFile(args(2))
    .getLines()
    .toIndexedSeq
    .map(s => {
      val tokens = s.split(":")
      val name = tokens(0)
      val templates = tokens(1).split(",")
      (name,templates)
    })
    .toMap
  val configDirs = templateRootDir.listFiles()
  configDirs.toIndexedSeq.foreach(dir => {
    val configName = dir.getName
    val resultDir = new File(rolesetRootDir.getAbsolutePath + s"/$configName/")
    resultDir.mkdirs()
    val templateDir = dir.getAbsolutePath + "/byTemplate/"
    datasetList.foreach{case (dsName,templates) => {
      val resultFile = new File(resultDir.getAbsolutePath + s"/$dsName.json")
      val roles = templates.flatMap(template => RoleLineageWithID.fromJsonObjectPerLineFile(templateDir + s"/$template.json"))
        .map(rl => (rl.id,rl))
        .toIndexedSeq
        .sortBy(_._1)
      val roleIDs = roles.map(_._1)
      val posToRLMap = roles.zipWithIndex.map{case ((_,rl),i) => (i,rl)}.toMap
      val roleset = Roleset(roleIDs,posToRLMap)
      roleset.toJsonFile(resultFile)
    }}
  })
  val resultFile = args(1)
}
