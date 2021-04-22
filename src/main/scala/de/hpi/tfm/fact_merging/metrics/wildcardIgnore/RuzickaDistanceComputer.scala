package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import de.hpi.tfm.compatibility.graph.fact.TupleReference

//https://en.wikipedia.org/wiki/Jaccard_index#Generalized_Jaccard_similarity_and_distance
class RuzickaDistanceComputer[A](tr1: TupleReference[A], tr2: TupleReference[A]) extends WildcardIgnoreHistogramBasedComputer[A](tr1,tr2){

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
