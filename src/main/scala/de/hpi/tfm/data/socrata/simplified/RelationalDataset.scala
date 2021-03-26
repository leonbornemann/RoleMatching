package de.hpi.tfm.data.socrata.simplified

import de.hpi.tfm.data.socrata
import de.hpi.tfm.data.socrata.change.DiffAsChangeCube
import de.hpi.tfm.data.socrata.{DatasetInstance, JsonReadable, JsonWritable}
import de.hpi.tfm.io.IOService
import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable

case class RelationalDataset(id:String,
                             version:LocalDate,
                             var attributes:collection.IndexedSeq[Attribute],
                             var rows:mutable.ArrayBuffer[RelationalDatasetRow]) extends JsonWritable[RelationalDataset] {
  def isEmpty: Boolean = rows.isEmpty


  def sortColumnsByAttributePosition() = {
    assert(attributes.map(_.position.get).sorted.toIndexedSeq == (0 until attributes.size))
    val oldAttributeOrder = attributes
    val newAttributeOrder = attributes.sortBy(_.position.get)
    val oldToNewOrder = oldAttributeOrder.zipWithIndex.map{case (a,oldIndex) => (oldIndex,a.position.get)}
        .toMap
    rows.foreach(r => r.reorderColumns(oldToNewOrder))
    attributes = newAttributeOrder
  }


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
    val f = IOService.getSimplifiedDatasetFile(socrata.DatasetInstance(id,version))
    if(new File(f).exists())
      Some(RelationalDataset.fromJsonFile(f))
    else
      None
  }

  def createEmpty(id: String, date: LocalDate): RelationalDataset = {
    RelationalDataset(id,date,IndexedSeq(),mutable.ArrayBuffer())
  }

}