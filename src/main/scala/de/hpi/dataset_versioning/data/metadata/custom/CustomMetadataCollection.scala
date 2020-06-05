package de.hpi.dataset_versioning.data.metadata.custom

import java.time.LocalDate

import de.hpi.dataset_versioning.data.{DatasetInstance, JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.IOService

case class CustomMetadataCollection(metadata:Map[DatasetInstance,CustomMetadata]) extends JsonWritable[CustomMetadataCollection]{

  //exclude creations at:
  val versionToIgnore = Seq("2020-03-07","2020-03-14","2020-03-21","2020-03-28","2020-04-04","2020-04-11","2020-04-18")
    .map(LocalDate.parse(_,IOService.dateTimeFormatter))

  val metadataByIntID = metadata.map{case(_,c) => (c.intID,c)}

  def getSortedVersions = {
    metadata
      .values
      .groupBy(cm => cm.id)
      .toIndexedSeq
      .map{case (id,mds) => (id,mds.toIndexedSeq
        .filter(c => !versionToIgnore.contains(c.version))
        .sortBy(_.version.toEpochDay))}
      .toMap
  }
}

object CustomMetadataCollection extends JsonReadable[CustomMetadataCollection]
