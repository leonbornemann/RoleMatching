package de.hpi.dataset_versioning.data.simplified

import java.io.{File, PrintWriter}
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import org.apache.commons.csv.{CSVFormat, CSVPrinter, CSVRecord}

import scala.jdk.CollectionConverters._

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

  def toCSV(file: File) = {
    val writer = new PrintWriter(file)
    val csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)
    csvPrinter.printRecord((attributes.map(_.name).toArray):_*)
    rows.foreach(r => {
      csvPrinter.printRecord((r.arraysToString.toArray ):_*)
    })
    csvPrinter.close(true)
  }


}

object RelationalDataset extends JsonReadable[RelationalDataset]