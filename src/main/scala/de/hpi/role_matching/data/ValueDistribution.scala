package de.hpi.role_matching.data

import scala.util.Random

case class ValueDistribution[T](distribution: Map[T, Int]) {

  def drawWeightedRandomValues(distinctValueCount: Any, random: Random) = {
    val res = collection.mutable.HashSet[T]()
    while(res.size!=distinctValueCount){
      val drawnValue = drawWeightedRandomValue(random)
      res.add(drawnValue)
    }
    res
  }


  val total = distribution.values.sum

  def getHist() = {
    var curCounter = 0
    val sorted = distribution
      .toIndexedSeq
      .sortBy(-_._2)
    val hist = collection.mutable.TreeMap[Int,T]()
    sorted.foreach{case (v,count) => {
      hist(curCounter) = v
      curCounter += count
    }}
    hist
  }

  val histogram = getHist()

  def drawWeightedRandomValue(random:Random) = {
    val toDraw = random.nextInt(total)
    if(histogram.contains(toDraw))
      histogram(toDraw)
    else
      histogram.maxBefore(toDraw).get._2
  }

}
object ValueDistribution {

  def fromIterable[T](iterable: Iterable[T]) = {
    ValueDistribution(iterable
      .groupBy(v => v)
      .map{case (t,arr) => (t,arr.size)})
  }

}

