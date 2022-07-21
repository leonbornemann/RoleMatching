package de.hpi.role_matching.wikipedia_data_preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.data
import de.hpi.role_matching.wikipedia_data_preparation.transformed.WikipediaRoleLineage
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate
import scala.io.Source

object TemplateDirToRolesetMain extends App with StrictLogging{
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
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
  val trainTimeEnd = LocalDate.parse(args(3))
  val configDirs = templateRootDir.listFiles()
  configDirs.toIndexedSeq.foreach(dir => {
    logger.debug(s"Processing $dir")
    val configName = dir.getName
    val resultDir = new File(rolesetRootDir.getAbsolutePath + s"/$configName/")
    resultDir.mkdirs()
    val templateDir = dir.getAbsolutePath + "/byTemplate/"
    datasetList.foreach{case (dsName,templates) => {
      logger.debug(s"Reading $dsName")
      val resultFile = new File(resultDir.getAbsolutePath + s"/$dsName.json")
      val roles = templates.flatMap(template => {
        val toRead = templateDir + s"/$template.json"
        logger.debug(s"Reading $toRead")
        WikipediaRoleLineage.fromJsonObjectPerLineFile(toRead)
          .withFilter(_.isOfInterest(trainTimeEnd))
          .map(_.toIdentifiedFactLineage)
      })
        .map(rl => (rl.id,rl))
        .toIndexedSeq
        .sortBy(_._1)
      val roleIDs = roles.map(_._1)
      val posToRLMap = roles.zipWithIndex.map{case ((_,rl),i) => (i,rl)}.toMap
      val roleset = data.Roleset(roleIDs,posToRLMap)
      roleset.toJsonFile(resultFile)
    }}
  })
  val resultFile = args(1)
}
