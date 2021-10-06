package de.hpi.role_matching.baseline

import scala.io.Source

object ValueSetBaselinePrecisionRecallComputationMain extends App{
  val file = "/home/leon/data/dataset_versioning/valueSequenceBaseline/result.txt"
  val lines = Source.fromFile(file).getLines().toIndexedSeq
    .zipWithIndex

  def getCount(tuple: (String, Int)) = {
    tuple._1.split(":")(1).trim.toInt
  }

  def getScaledDoubleString(d: Double) = {
    BigDecimal(d).setScale(2,BigDecimal.RoundingMode.HALF_UP)
  }

  def printCSVString(value: IndexedSeq[(String, Int)], dsName: String) = {
    val cliquePrecision = getScaledDoubleString(getCount(value(4)) / (getCount(value(4)) + getCount(value(5))).toDouble)
    val precision = getCount(value(0)) / (getCount(value(0)) + getCount(value(1))).toDouble
    val recall = getCount(value(2)) / (getCount(value(2)) + getCount(value(3))).toDouble
    val f1 = getScaledDoubleString(2*recall*precision/(recall+precision).toDouble)
    println(s"$dsName,$cliquePrecision,${getScaledDoubleString(precision)},${getScaledDoubleString(recall)},$f1")
  }

  lines.foreach{case (l,i) => {
    if(l.contains("/")){
      val dsName = l.split("/")(3)
      printCSVString(lines.slice(i+1,i+7),dsName)
    }
  }}

}
