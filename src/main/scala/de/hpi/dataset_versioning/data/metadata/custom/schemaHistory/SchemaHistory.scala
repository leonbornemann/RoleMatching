package de.hpi.dataset_versioning.data.metadata.custom.schemaHistory

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.simplified.{Attribute, RelationalDataset}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

case class SchemaHistory(datasetID:String,superSchema:Set[Attribute]) extends JsonWritable[SchemaHistory] with StrictLogging

object SchemaHistory extends JsonReadable[SchemaHistory] with StrictLogging {

  def load(str: String) = fromJsonFile(IOService.getSchemaHistoryFile(str).getAbsolutePath)

  def loadAll() = {
    val files = IOService.getSchemaHistoryDir().listFiles()
        .filter(_.getName.endsWith(".json"))
    files.map(f => (IOService.filenameToID(f),load(IOService.filenameToID(f))))
      .toMap
  }


  def buildFromDatasetVersions(h:DatasetVersionHistory): SchemaHistory = {
    val schemaMap = mutable.HashMap[Int,Attribute]()
    h.versionsWithChanges.foreach(v => {
      val curDs = RelationalDataset.load(h.id,v)
      val curSchema = curDs.attributes
      curSchema.foreach(a => {
        if(!schemaMap.contains(a.id))
          schemaMap.put(a.id,a)
      })
    })
    SchemaHistory(h.id,schemaMap.values.toSet)
  }

  def exportAllSchemaHistories() = {
    val versionHistories = DatasetVersionHistory.load()
    var count = 0
    versionHistories.foreach(h => {
      val targetFile = IOService.getSchemaHistoryFile(h.id)
      if(!targetFile.exists()) {
        val schemaHistory: SchemaHistory = buildFromDatasetVersions(h)
        schemaHistory.toJsonFile(targetFile, true)
        count += 1
        if (count % 1000 == 0)
          logger.debug(s"processed $count histories")
      }
    })
  }

}
