package de.hpi.dataset_versioning.data.metadata.custom

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable.HashMap

object CustomMetadataExportMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val startVersion = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val endVersion = LocalDate.parse(args(2),IOService.dateTimeFormatter)
  //read
  export()

  private def read = {
    val mdColelction = CustomMetadataCollection.fromJsonFile(IOService.getCustomMetadataFile(startVersion,endVersion).getAbsolutePath)
    val histogram = mdColelction.metadata.values.groupBy(_.tupleSpecificHash)
      .mapValues(_.size)
      .values.toSeq
      .groupBy(identity)
      .mapValues(_.size)
    histogram.foreach(println(_))
  }

  def export() = {
    //TODO: make this part of a date range
    val files = IOService.extractMinimalHistoryInRange(startVersion,endVersion)
    val metadataCollection = HashMap[DatasetInstance,CustomMetadata]()
    var intID = 0
    files.foreach{case (v,f) => {
      val ds = IOService.tryLoadDataset(new DatasetInstance(IOService.filenameToID(f),v),true)
      if(!ds.isEmpty){
        val md = ds.extractCustomMetadata(intID)
        metadataCollection.put(data.DatasetInstance(IOService.filenameToID(f),v),md)
      }
      intID +=1
      if(intID%1000 == 0)
        logger.debug(s"finished $intID")
    }}
    CustomMetadataCollection(metadataCollection.toMap).toJsonFile(IOService.getCustomMetadataFile(startVersion,endVersion))
  }



}
