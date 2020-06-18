package de.hpi.dataset_versioning.data.simplified

import java.io.{File, PrintWriter}
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.DiffAsChangeCube
import de.hpi.dataset_versioning.data.{DatasetInstance, JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.io.IOService
import org.apache.commons.csv.{CSVFormat, CSVPrinter, CSVRecord}

import scala.jdk.CollectionConverters._

case class RelationalDataset(id:String,
                             version:LocalDate,
                             var attributes:collection.IndexedSeq[Attribute],
                             rows:collection.IndexedSeq[RelationalDatasetRow]) extends JsonWritable[RelationalDataset] {

  def rowsAreMatched: Boolean = rows.forall(r => r.id != -1)


  def calculateDataDiff(nextVersion: RelationalDataset) = {
    DiffAsChangeCube.fromDatasetVersions(this,nextVersion)
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

object RelationalDataset extends JsonReadable[RelationalDataset] {

  def load(id:String,version:LocalDate) = {
    RelationalDataset.fromJsonFile(IOService.getSimplifiedDatasetFile(DatasetInstance(id,version)))
  }

  def tryLoad(id:String,version:LocalDate) = {
    val f = IOService.getSimplifiedDatasetFile(DatasetInstance(id,version))
    if(new File(f).exists())
      Some(RelationalDataset.fromJsonFile(f))
    else
      None
  }

  def createEmpty(id: String, date: LocalDate): RelationalDataset = {
    RelationalDataset(id,date,IndexedSeq(),IndexedSeq())
  }

}