package de.hpi.oneshot

import scala.io.Source

object DittoEpochF1Processing extends App {
  val datasets = Seq("oregon","austintexas","chicago","utah","maryland")
  private val lines: IndexedSeq[String] = Source.fromFile("src/main/resources/tmp/dittoOutputSocrataNew.txt")
    .getLines()
    .toIndexedSeq
  //pivot file

  def printLines(lines: IndexedSeq[String], pivot: Boolean) = {
    if(pivot)
      println("dataset,epoch,scoreType,value")
    else
      println("dataset,epoch,valid_f1,test_f1,best_test_f1")
    var curDSIndex = -1
    lines
      .foreach(l => {
        val tokens = l.split(":")
        val epoch = tokens(0).split("\\s+")(1).toInt
        if(epoch==1)
          curDSIndex+=1
        val tokens1 = tokens(1).split(",")
        val dev_f1 = tokens1(0).split("=")(1).toDouble
        val f1 = tokens1(1).split("=")(1).toDouble
        val bestF1 = tokens1(2).split("=")(1).toDouble
        if(pivot){
          println(s"${datasets(curDSIndex)},$epoch,dev_f1,$dev_f1")
          println(s"${datasets(curDSIndex)},$epoch,f1,$f1")
          println(s"${datasets(curDSIndex)},$epoch,bestF1,$bestF1")
        } else {
          println(s"${datasets(curDSIndex)},$epoch,$dev_f1,$f1,$bestF1")
        }
      })
  }

  printLines(lines,false)
  println()
  printLines(lines,true)
}
