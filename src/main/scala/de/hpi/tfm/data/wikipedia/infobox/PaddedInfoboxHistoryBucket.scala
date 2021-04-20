package de.hpi.tfm.data.wikipedia.infobox

import java.io.{File, PrintWriter}
import scala.collection.immutable

case class PaddedInfoboxHistoryBucket(paddedHistories: IndexedSeq[PaddedInfoboxHistory], originalFileName: String) {
  val pageMin = BigInt(originalFileName.split("xml-p")(1).split("p")(0))
  val pageMax = BigInt(originalFileName.split("xml-p")(1).split("p")(1).split("\\.")(0))
  paddedHistories.foreach(ph => {
    assert(ph.pageID>=pageMin && ph.pageID<=pageMax)
  })

  def getBucketFilename = s"$pageMin-$pageMax.json"

  def writeToDir(dir:File) = {
    val pr = new PrintWriter(dir.getAbsolutePath + s"/$getBucketFilename")
    paddedHistories.foreach(ph => {
      ph.appendToBucketFile(pr)
    })
  }
}
