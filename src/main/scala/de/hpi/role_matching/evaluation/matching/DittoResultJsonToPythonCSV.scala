package de.hpi.role_matching.evaluation.matching

import java.io.{File, PrintWriter}
import scala.io.Source

object DittoResultJsonToPythonCSV extends App {
  val dittoDir = new File(args(0))
  val testFileDir = new File(args(1))
  val resultDir = new File(args(2))
  resultDir.mkdirs()
  dittoDir
    .listFiles()
    .foreach(f => {
      val filename = f.getName.split("\\.")(0)
      val dsName = if(!filename.startsWith("tv")) filename.split("_")(0) else "tv_and_film"
      val resultPR = new PrintWriter(resultDir + s"/${dsName}.csv")
      DittoResult.appendSchema(resultPR)
      val dittoTestFile = testFileDir.getAbsolutePath + s"/$dsName.json.txt_test.txt"
      val isMatchIterator = Source
        .fromFile(dittoTestFile)
        .getLines()
        .map(l => l.split("\\t")(2).toInt==1)
      val dittoResultIterator = DittoResult
        .iterableFromJsonObjectPerLineFile(f.getAbsolutePath)
      dittoResultIterator
        .zip(isMatchIterator)
        .foreach{case (dr,isMatch) => resultPR.println(dr.toCSVLine(isMatch))}
      resultPR.close()
    })

}
