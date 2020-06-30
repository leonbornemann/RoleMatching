package de.hpi.dataset_versioning.db_synthesis.top_down.main

import java.io.{File, PrintWriter}
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.{Change, ChangeCube}
import de.hpi.dataset_versioning.data.change.ChangeExportMain.subdomain
import de.hpi.dataset_versioning.db_synthesis.top_down.decomposition.DatasetInfo
import de.hpi.dataset_versioning.io.IOService

object ChangeExplorationMain extends App with StrictLogging{
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val filteredChangeFile = new File(IOService.TMP_WORKING_DIR + "filteredChanges.json")
  //val changeCubes = writeFilteredChanges
  val changeCubes = ChangeCube.fromJsonObjectPerLineFile(filteredChangeFile.getAbsolutePath)
  logger.debug(s"Registered ${changeCubes.map(_.deletes.size.toLong).sum} deletes")
  logger.debug(s"Registered ${changeCubes.map(_.inserts.size.toLong).sum} inserts")
  logger.debug(s"Registered ${changeCubes.map(_.updates.size.toLong).sum} updates")
  val attrGroups = changeCubes.flatMap(cc => cc.allChanges.collect({
    case c if (c.newValue!=None) => {
    val byVersion = cc.colIDTOAttributeMap(c.pID)
      byVersion(c.t).name}
  }).toSet.toSeq)
  logger.debug("By attribute name:")
  attrGroups.groupBy(identity)
    .toIndexedSeq
    .sortBy(-_._2.size)
    .foreach{case(a,attr) => println(s"$a:${attr.size}")}
  logger.debug("By human readable name:")
//  attrGroups.groupBy(_.humanReadableName)
//    .toIndexedSeq
//    .sortBy(-_._2.size)
//    .foreach{case(a,attr) => logger.debug(s"$a:${attr.size}")}
//    //.groupBy(s => s)

  //ccs.flatMap(cc => (cc.datasetID,cc.allChanges.map(_.)))


  private def writeFilteredChanges(outFile:File) = {
    val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
    val subdomainIds = subDomainInfo(subdomain)
      .map(_.id)
    var count = 0
    val changeCubes = subdomainIds.map(id => {
      logger.debug(s"Loading changes for $id")
      val changeCube = ChangeCube.load(id)
      val firstTimestamp = changeCube.firstTimestamp
      if (firstTimestamp.isDefined)
        changeCube.filterChangesInPlace((c: Change) => c.t != firstTimestamp.get)
      logger.debug(s"Loaded $count/${subdomainIds.size} changes")
      count += 1
      changeCube
    })
    val convenienceFileWriter = new PrintWriter(outFile)
    changeCubes.foreach(cc => cc.appendToWriter(convenienceFileWriter, false, true, true))
    convenienceFileWriter.close()
    changeCubes
  }
}
