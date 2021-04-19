package de.hpi.tfm.data.wikipedia.infobox

import java.io.File
import java.time.ZoneOffset
import scala.io.Source

object MainClass extends App {
  //https://owncloud.hpi.de/s/H2juuaquPE7BUAV/download?path=%2F&files=enwiki-20190901-pages-meta-history27.xml-p57135490p57467999.output.json.7z
  val file1 = Source.fromFile("/home/leon/data/dataset_versioning/WIkipedia/infoboxes/owncloud files")
    .getLines()
    .toIndexedSeq
    .filter(_.contains("enwiki-20190901-pages-meta"))
    .map(l => "https://owncloud.hpi.de/s/H2juuaquPE7BUAV/download?path=%2F&files=" + l.split("Aktionen")(0))
    .foreach(println)
  assert(false)
  val file = args(0)
  val resultDir = new File(args(1))
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(file)
  objects.foreach(_.checkIntegrity())
  val paddedHistory = InfoboxRevision
    .toPaddedInfoboxHistory(objects)
    .toIndexedSeq
  InfoboxRevisionBucket(paddedHistory,new File(file).getName)
    .writeToDir(resultDir)
}
