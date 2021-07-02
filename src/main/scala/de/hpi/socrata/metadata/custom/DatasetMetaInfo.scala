package de.hpi.socrata.metadata.custom

import de.hpi.socrata.io.Socrata_IOService.DATASET_METAINFO_DIR
import de.hpi.socrata.io.Socrata_Synthesis_IOService.createParentDirs
import de.hpi.socrata.{JsonReadable, JsonWritable}

import java.io.File

case class DatasetMetaInfo(id:String,name:String,description:String,associationInfo:IndexedSeq[AssociationMetaInfo]) extends JsonWritable[DatasetMetaInfo] {
  def associationInfoByID = associationInfo.map(a => (a.id,a)).toMap


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
