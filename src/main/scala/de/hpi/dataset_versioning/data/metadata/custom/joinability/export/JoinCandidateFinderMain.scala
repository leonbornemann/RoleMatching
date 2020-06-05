package de.hpi.dataset_versioning.data.metadata.custom.joinability.`export`

import java.io.PrintWriter
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.io.IOService

object JoinCandidateFinderMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val startVersion = LocalDate.parse("2019-11-01",IOService.dateTimeFormatter)
  val endVersion = LocalDate.parse("2020-04-30",IOService.dateTimeFormatter)
  logger.debug("beginning metadata read")
  IOService.cacheCustomMetadata(startVersion,endVersion)
  logger.debug("end metadata read")
  val resultFile = new PrintWriter(IOService.getJoinCandidateFile())
  resultFile.println("intID,strID,version")
  val lastVersionToConsider = LocalDate.parse("2019-11-30",IOService.dateTimeFormatter)
  val firstDataLakeVersion = IOService.getSortedDatalakeVersions.head
  val sortedVersionLists = IOService.cachedCustomMetadata((startVersion,endVersion))
    .metadata
    .values
    .groupBy(cm => cm.id)
    .toIndexedSeq
    .map{case (id,mds) => (id,mds.toIndexedSeq
      .filter(cm => cm.version.toEpochDay<=lastVersionToConsider.toEpochDay)
      .sortBy(_.version.toEpochDay))}
    .filter(_._2.size >=1)
  sortedVersionLists.foreach{ case (id,list) => {
    var prevColCount = list(0).ncols
    var prevSchemaSet = list(0).columnMetadata.keySet
    if(list(0).version!=firstDataLakeVersion) {
      //insert of new dataset
      val curMD = list(0)
      resultFile.println(s"${curMD.intID},${curMD.id},${curMD.version.format(IOService.dateTimeFormatter)}")
    }
    for(i <- 1 until list.size){
      val curColCount = list(i).ncols
      val curSchemaSet = list(i).columnMetadata.keySet
      if((curColCount > prevColCount || prevSchemaSet!= curSchemaSet) && prevColCount >= 2 && curColCount >= 3 ){
        //found a candidate!
        val curMD = list(i)
        resultFile.println(s"${curMD.intID},${curMD.id},${curMD.version.format(IOService.dateTimeFormatter)}")
      }
      prevColCount = curColCount
      prevSchemaSet = curSchemaSet
    }
  }}
  resultFile.close()
}
