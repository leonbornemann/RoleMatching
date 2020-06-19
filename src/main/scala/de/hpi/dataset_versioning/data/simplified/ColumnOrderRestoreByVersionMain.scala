package de.hpi.dataset_versioning.data.simplified

import java.io.File
import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object ColumnOrderRestoreByVersionMain extends App with StrictLogging{

  IOService.socrataDir = args(0)
  val version = LocalDate.parse(args(1),IOService.dateTimeFormatter)
  val id = if(args.length==2) Some(args(2)) else None
  val restorer = new ColumnOrderRestorer()
  if(id.isDefined){
    logger.debug(s"Redoing Column Ordering for single dataset version: $id and $version")
    restorer.restoreInDataset(id.get,version)
  } else {
    logger.debug(s"Redoing Column Ordering for all datasets in version  $version")
    restorer.restoreAllInVersion(version)
  }


}
