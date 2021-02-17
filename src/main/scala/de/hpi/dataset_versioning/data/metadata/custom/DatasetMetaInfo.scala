package de.hpi.dataset_versioning.data.metadata.custom

import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema
import de.hpi.dataset_versioning.io.DBSynthesis_IOService.createParentDirs
import de.hpi.dataset_versioning.io.IOService.DATASET_METAINFO_DIR

import java.io.File

case class DatasetMetaInfo(id:String,name:String,description:String,associationInfo:IndexedSeq[AssociationMetaInfo]) extends JsonWritable[DatasetMetaInfo] {

  def writeToStandardFile() = toJsonFile(DatasetMetaInfo.getStandardFile(id))

}

object DatasetMetaInfo extends JsonReadable[DatasetMetaInfo]{
  val NO_DESCRIPTION = "<No Description Available>"

  def readAll(subdomain:String) = {
    createParentDirs(new File(DATASET_METAINFO_DIR)).listFiles().map(f => fromJsonFile(f.getAbsolutePath))
  }

  def readFromStandardFile(id:String) = fromJsonFile(getStandardFile(id).getAbsolutePath)

  def getStandardFile(id: String) = createParentDirs(new File(DATASET_METAINFO_DIR + s"/$id.json"))
}
