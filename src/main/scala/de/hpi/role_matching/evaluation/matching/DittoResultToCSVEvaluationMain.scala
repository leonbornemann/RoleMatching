package de.hpi.role_matching.evaluation.matching

import java.io.{File, PrintWriter}
import scala.io.Source

object DittoResultToCSVEvaluationMain extends App {
  val dittoTestFileDir = args(0)
  val dittoResultFileDir = args(1)
  val datasetsToEvaluate = args(2).split(",")
  val csvExportDir = args(3)

  def getClassLabel(l: String) = {
    l.split("\t").last.toInt
  }

  datasetsToEvaluate.foreach(ds => {
    val testFile = new File(dittoTestFileDir + s"/$ds.txt_test.txt")
    val resultFile = new File(dittoResultFileDir + s"/${ds}_result.json")
    val csvResultFile = new PrintWriter(dittoResultFileDir + s"/${ds}.csv")
    val iterable = DittoResultLine.iterableFromJsonObjectPerLineFile(resultFile.getAbsolutePath)
    csvResultFile.println(LabelledDittoResult.schema)
    Source.fromFile(testFile).getLines().foreach(l => {
      assert(iterable.hasNext)
      val correspondingResult = iterable.next()
      val trueClassLabel = getClassLabel(l)
      csvResultFile.println(LabelledDittoResult(ds, correspondingResult._match == 1, trueClassLabel == 1, correspondingResult.match_confidence).csvLine)
    })
    csvResultFile.close()
  })

}
