package de.hpi.dataset_versioning.db_synthesis.evaluation

case class Histogram(values: collection.Seq[Int]) {

  val hist = values.groupBy(identity)
    .map(t => (t._1,t._2.size))
    .toIndexedSeq
    .sortBy(_._1)

  def printAll() = {
    println("--------------------")
    hist.foreach(t => {
      println(s"${t._1}: ${t._2}")
    })
    println("--------------------")
  }
}
