package de.hpi.dataset_versioning.oneshot

import scala.io.Source

object ScriptMain3 extends App {

  val path = "/home/leon/data/dataset_versioning/plotData/pkFIeldChanges/"
  val path1 = path + "NormalFieldChangeCounter_DeterminantOnly"
  val path2 = path + "NormalFieldChangeCounter_NonDeterminantOnly"

  def readNumbers(path1: String) = {
    Source.fromFile(path1).getLines().toIndexedSeq.map(_.toDouble)
  }

  val values1 = readNumbers(path1)
  val values2 = readNumbers(path2)
  val avg1 = getAVG(values1)

  def getAVG(values2: IndexedSeq[Double]) = {
    val filtered = values2.filter(d => !d.isNaN && d!=0.0)
    filtered.sum / filtered.size
  }

  val avg2 = getAVG(values2)
  println(avg1)
  println(avg2)
}
