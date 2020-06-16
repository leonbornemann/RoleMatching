package de.hpi.dataset_versioning.data.simplified

import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.io.IOService

object ColumnOrderRestore extends App {

  IOService.socrataDir = args(0)
  val version = LocalDate.parse("2019-11-01",IOService.dateTimeFormatter)
  val dir = IOService.getSimplifiedDataDir(version)
  IOService.cacheMetadata(version)
  var attrPositionFound:Long = 0
  var attrPositionNotFound:Long = 0
  dir.listFiles.foreach(f => {
    val id = IOService.filenameToID(f)
    val ds = IOService.loadSimplifiedRelationalDataset(DatasetInstance(id,version))
    val mdForDS = IOService.cachedMetadata(version)(id)
    val baseNameToPosition = mdForDS.resource.columnNameToPosition
    var interesting:Any = false
    ds.attributes.foreach(a => {
      if(baseNameToPosition.contains(a.name)) {
        a.position = Some(baseNameToPosition(a.name))
        attrPositionFound +=1
      } else if (a.name.startsWith("_") && baseNameToPosition.contains(a.name.substring(1))) {
        a.position = Some(baseNameToPosition(a.name.substring(1)))
        attrPositionFound +=1
      } else {
        val tokens = a.name.split("_")
        if (a.name.startsWith("_") && baseNameToPosition.contains(tokens(tokens.size-1))) {
          a.position = Some(baseNameToPosition(tokens(tokens.size-1)))
          attrPositionFound +=1
        } else {
          interesting = a
          attrPositionNotFound +=1
        }
      }
    })
    if(interesting.isInstanceOf[Attribute]){
      println(s"$attrPositionFound,$attrPositionNotFound")
    }
  })
}
