package de.hpi.tfm.data.wikipedia.infobox

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.evaluation.Histogram

import java.io.File
import java.time.ZoneOffset
import scala.io.Source

object PaddedInfoboxHistoryCreation extends App with StrictLogging{
  //https://owncloud.hpi.de/s/H2juuaquPE7BUAV/download?path=%2F&files=enwiki-20190901-pages-meta-history27.xml-p57135490p57467999.output.json.7z
//  val file1 = Source.fromFile("/home/leon/data/dataset_versioning/WIkipedia/infoboxes/owncloud files")
//    .getLines()
//    .toIndexedSeq
//    .filter(_.contains("enwiki-20190901-pages-meta"))
//    .map(l => "https://owncloud.hpi.de/s/H2juuaquPE7BUAV/download?path=%2F&files=" + l.split("Aktionen")(0))
//    .foreach(println)
//  assert(false)
  val file = args(0)
  val resultDir = new File(args(1))
  val objects = InfoboxRevision.fromJsonObjectPerLineFile(file)
  objects.foreach(_.checkIntegrity())
  val revisionHistories = InfoboxRevisionHistory.getFromRevisionCollection(objects)
  revisionHistories.foreach(rh => rh.integrityCheck())
//  revisionHistories.filter(_.key=="41149491-0").head
//    .toPaddedInfoboxHistory
  logger.debug(s"Found ${revisionHistories.size} infobox histories to process")
  var finished = 0
  val paddedHistories = revisionHistories
    .map(r => {
      val res = r.toPaddedInfoboxHistory
      finished +=1
      if(finished % 100 ==0) {
        logger.debug(s"Finished $finished infobox histories")
      }
      res
    })
    .toIndexedSeq
  PaddedInfoboxHistoryBucket(paddedHistories,new File(file).getName)
    .writeToDir(resultDir)
}
