package de.hpi.role_matching.cbrm.sgcp

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}

case class Histogram(values: collection.Seq[Int], relative: Boolean = false) extends JsonWritable[Histogram]{

  var hist: IndexedSeq[(Int, Double)] = values.groupBy(identity)
    .map(t => (t._1, t._2.size.toDouble))
    .toIndexedSeq
    .sortBy(_._1)
  if (relative) {
    hist = hist.map(t => (t._1, t._2 / values.size.toDouble))
  }

  def printAll() = {
    println("--------------------")
    hist.foreach(t => {
      println(f"${t._1}: ${t._2}%.5f")
    })
    println("--------------------")
  }
}
object Histogram extends JsonReadable[Histogram]