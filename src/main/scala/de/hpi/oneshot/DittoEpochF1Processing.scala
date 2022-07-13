package de.hpi.oneshot

import scala.io.Source

object DittoEpochF1Processing extends App {
  //val datasets = Seq("oregon","austintexas","chicago","utah","maryland")
  //tasknames=("education_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" "military_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" "politics_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" "tv_and_film_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore"
  // "oregon_withIDAndScore" "chicago_withIDAndScore" "utah_withIDAndScore" "gov.maryland_withIDAndScore" "austintexas_withIDAndScore" "football_PROBABILISTIC_DECAY_FUNCTION_0.7_7_2016-05-07_withIDAndScore" )
  //val datasets = Seq("education","military","politics","tv_and_film","oregon","chicago","utah","maryland","austintexas","football")
  val datasets = Seq("education","military","politics","tv_and_film","football")
  private val lines: IndexedSeq[String] = Source.fromFile("src/main/resources/tmp/dittoOutputWikipediaNew.txt") // Source.fromFile("src/main/resources/tmp/dittoOutputAllWithIDAndScore.txt")
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
