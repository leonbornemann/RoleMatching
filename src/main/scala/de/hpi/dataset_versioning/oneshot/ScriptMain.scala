package de.hpi.dataset_versioning.oneshot

import java.io.{File, FileReader}
import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.{ChangeCube, ChangeExporter, TemporalTable}
import de.hpi.dataset_versioning.data.simplified.RelationalDataset
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.FunctionalDependencySet
import de.hpi.dataset_versioning.io.IOService
import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.collection.mutable
import scala.io.Source

object ScriptMain extends App {
  IOService.socrataDir = args(0)
  private val date = LocalDate.parse("2019-12-12")
  val ds = RelationalDataset.load("qhb4-qx8k",date) //ID 21 does not exist in this snapshot!
  //val exporter = new ChangeExporter()
  //exporter.exportAllChanges("qhb4-qx8k")
  val temporalTable = TemporalTable.load("qhb4-qx8k")
  val as =temporalTable.attributes.map(_.valueAt(date))
  val f = new File("/home/leon/data/dataset_versioning/socrata/fromServer/db_synthesis/decomposition/csv/org.cityofchicago/qhb4-qx8k/2019-12-12.csv")
  val csvReader = CSVParser.parse(new FileReader(f),CSVFormat.DEFAULT)
  var cells = mutable.ArrayBuffer[mutable.ArrayBuffer[String]]()
  csvReader.getRecords.forEach(r => {
    val curList = mutable.ArrayBuffer[String]()
    r.forEach(c => curList.addOne(c))
    cells.addOne(curList)
  })
  //val indices = IndexedSeq[Int](1,8)
  val indices = IndexedSeq[Int](0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22)
  val grouped = cells.groupBy(r => indices.map(i => r(i)))
  println()
  grouped.filter(_._2.size>1)
    .foreach(g => {
      println("--------------------------------------------------------------------------")
      println(g._1)
      g._2.foreach(println(_))
    })
}
