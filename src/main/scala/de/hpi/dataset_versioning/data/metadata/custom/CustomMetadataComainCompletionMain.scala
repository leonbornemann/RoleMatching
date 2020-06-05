package de.hpi.dataset_versioning.data.metadata.custom

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

object CustomMetadataComainCompletionMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  logger.debug("Loading Custom Metadata")
  val mdCollection = IOService.getOrLoadCustomMetadataForStandardTimeFrame()
  logger.debug("Finished Loading Custom Metadata")
  var curDate = IOService.STANDARD_TIME_FRAME_START
  val endDate = IOService.STANDARD_TIME_FRAME_END
  while (curDate.toEpochDay <=endDate.toEpochDay){
    logger.debug(s"Dealing with $curDate")
    IOService.cacheMetadata(curDate)
    val curMd = IOService.cachedMetadata(curDate)
    curMd.keySet.foreach(k => {
      if(mdCollection.metadata.contains(DatasetInstance(k,curDate)))
        mdCollection.metadata(DatasetInstance(k,curDate)).link = Some(curMd(k).link)
    })
    IOService.cachedMetadata.clear()
    curDate = curDate.plusDays(1)
  }
  val file = IOService.getCustomMetadataFile(IOService.STANDARD_TIME_FRAME_START,IOService.STANDARD_TIME_FRAME_END).getAbsolutePath + "_updated"
  mdCollection.toJsonFile(new File(file))
}
