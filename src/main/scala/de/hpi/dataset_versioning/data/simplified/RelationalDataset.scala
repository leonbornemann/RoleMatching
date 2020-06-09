package de.hpi.dataset_versioning.data.simplified

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}

case class RelationalDataset(id:String,
                             version:LocalDate,
                             attributes:collection.IndexedSeq[Attribute],
                             rows:collection.IndexedSeq[RelationalDatasetRow]) extends JsonWritable[RelationalDataset] {

  def calculateDataDiff(nextVersion: RelationalDataset) = {
    ChangeCube.fromDatasetVersions(this,nextVersion)
  }

  def getAttributesByID = {
    attributes.map(a => (a.id,a))
      .toMap
  }

}

object RelationalDataset extends JsonReadable[RelationalDataset]