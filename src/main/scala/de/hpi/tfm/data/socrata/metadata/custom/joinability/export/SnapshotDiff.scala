package de.hpi.tfm.data.socrata.metadata.custom.joinability.`export`

import de.hpi.tfm.io.IOService

import java.io.File
import java.time.LocalDate
import scala.collection.mutable
import scala.io.Source

class SnapshotDiff(version:LocalDate, diffDir: File) {

  val files = diffDir.listFiles()

  val createdDatasetFiles = mutable.ArrayBuffer[File]()
  private val shouldBeCreated = mutable.ArrayBuffer[String]()
  val deletedDatasetIds = mutable.ArrayBuffer[String]()
  val diffFiles = mutable.ArrayBuffer[File]()
  files.foreach(f => {
    if(f.getName =="created.meta"){
      shouldBeCreated ++= Source.fromFile(f).getLines().toSeq.map(_.split("\\.")(0))
    } else if(f.getName== "deleted.meta")
      deletedDatasetIds ++= Source.fromFile(f).getLines().toSeq.map(_.split("\\.")(0))
    else if(f.getName.endsWith(".json?")){
      createdDatasetFiles += f
    } else{
      assert(f.getName.endsWith(".diff"))
      diffFiles += f
    }
  })
  assert(createdDatasetFiles.map(IOService.filenameToID(_)).toSet == shouldBeCreated.toSet)
  assert(createdDatasetIds.intersect(changedDatasetIds).isEmpty)

  def createdDatasetIds = createdDatasetFiles.map(IOService.filenameToID(_))
  def changedDatasetIds = diffFiles.map(IOService.filenameToID(_))


}
