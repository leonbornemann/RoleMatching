package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait

//https://en.wikipedia.org/wiki/Jaccard_index#Generalized_Jaccard_similarity_and_distance
class RuzickaDistanceComputer[A](f1: TemporalFieldTrait[A], f2: TemporalFieldTrait[A],TIMESTAMP_RESOLUTION_IN_DAYS:Long) extends WildcardIgnoreHistogramBasedComputer[A](f1,f2,TIMESTAMP_RESOLUTION_IN_DAYS){

  def computeScore(): Double = {
    val (nominator,denominator) = hist1.keySet.union(hist2.keySet)
      .map(t => {
        val countA = hist1.getOrElse(t,Seq()).size
        val countB = hist2.getOrElse(t,Seq()).size
        (Math.min(countA,countB),Math.max(countA,countB))
      })
      .reduce((a,b) => (a._1+b._1,a._2+b._2))
    nominator / denominator.toDouble
  }


}
