package de.hpi.tfm.evaluation

case class AggregatedHistogram(values:collection.Map[Int,Int]){

  var hist:IndexedSeq[(Int,Int)] = values
    .toIndexedSeq
    .sortBy(_._1)

  def printAll() = {
    println("--------------------")
    hist.foreach(t => {
      println(f"${t._1}: ${t._2}%.5f")
    })
    println("--------------------")
  }

}
