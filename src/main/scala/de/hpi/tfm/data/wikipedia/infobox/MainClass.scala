package de.hpi.tfm.data.wikipedia.infobox

import java.io.File
import java.time.ZoneOffset
import scala.io.Source

object MainClass extends App {
//  val file1 = Source.fromFile("/home/leon/data/dataset_versioning/WIkipedia/infoboxes/owncloud files")
//    .getLines()
//    .toIndexedSeq
//    .filter(_.contains("enwiki-20190901-pages-meta"))
//    .map(l => "https://owncloud.hpi.de/s/oLQ7zrblMJHvNqd/download?path=%2F&files=" + l.split("Aktionen")(0))
//    .foreach(println)
//  assert(false)
  val file = "/home/leon/data/dataset_versioning/WIkipedia/infoboxes/sample/enwiki-20171103-pages-meta-history10.xml-p2336425p2370393.output.json"
  val resultDir = "/home/leon/data/dataset_versioning/WIkipedia/infoboxes/jsonResults/"
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(file)
  val earliest = objects.map(_.validFromAsDate).minBy(_.toEpochSecond(ZoneOffset.UTC))
  val latest = objects.map(_.validFromAsDate).maxBy(_.toEpochSecond(ZoneOffset.UTC))
  println(InfoboxRevision.formatter.format(earliest))
  println(earliest)
  println(latest)
  objects.foreach(_.checkIntegrity())
  val paddedHistory = InfoboxRevision.toPaddedInfoboxHistory(objects)
  paddedHistory.foreach(ph => ph.writeToDir(new File(resultDir)))
  println()
}
