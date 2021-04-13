package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.compatibility.graph.fact.{FactMergeabilityGraphEdge, TupleReference}
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

class GreedyEdgeWeightOptimizerForComponent(val subGraph: Graph[TupleReference[Any], WLkUnDiEdge]) {

  private var MIN_EDGE_WEIGHT_THRESHOLD = 0.0

  val adjacencyList = subGraph.edges.map(e => {
    val edgeObject = e.label.asInstanceOf[FactMergeabilityGraphEdge]
    (Set(edgeObject.tupleReferenceA,edgeObject.tupleReferenceB),e)
    })
    .toMap

  val merges = scala.collection.mutable.HashMap() ++ subGraph.nodes
    .map(n => (n.value.toIDBasedTupleReference,TupleMerge(Set(n.value.toIDBasedTupleReference),GLOBAL_CONFIG.OPTIMIZATION_TARGET_FUNCTION(n.value))))
    .toMap

  def executeMergeIfPossible(curEdge: FactMergeabilityGraphEdge) = {
    val v1 = curEdge.tupleReferenceA
    val v2 = curEdge.tupleReferenceB
    val curV1Clique = merges(v1).clique
    val curV2Clique = merges(v2).clique
    if(curV1Clique==curV2Clique){
      //do nothing
    } else {
      if(!curV1Clique.intersect(curV2Clique).isEmpty){
        println()
      }
      assert(curV1Clique.intersect(curV2Clique).isEmpty)
      var isClique = true
      var newScore = 0.0
      val it1 = curV1Clique.iterator
      var it2 = curV2Clique.iterator
      var n1 = it1.next()
      while(isClique && it2.hasNext){
        val n2 = it2.next()
        val edge = adjacencyList.get(Set(n1,n2))
        if(edge.isEmpty){
          //early abort
          isClique = false
        } else {
          newScore += edge.get.weight
        }
        if(!it2.hasNext && it1.hasNext){
          //move first iterator to next, restart second iterator
          n1 = it1.next()
          it2 = curV2Clique.iterator
        }
      }
      if(isClique){
        //executeMerge
        val mergedClique = curV1Clique.union(curV2Clique)
        val newMerge = TupleMerge(mergedClique,merges(v1).score + merges(v2).score + newScore)
        mergedClique.foreach(v => {
          merges(v) = newMerge
        })
      }
    }
  }

  def mergeGreedily() = {
    val edges = subGraph.edges.toIndexedSeq
      .sortBy(-_.weight)
      .iterator
    var done = false
    while(edges.hasNext && !done){
      val curEdgeObject = edges.next()
      val curEdge = curEdgeObject.label.asInstanceOf[FactMergeabilityGraphEdge]
      if(curEdgeObject.weight<=MIN_EDGE_WEIGHT_THRESHOLD){
        done = true
      } else {
        executeMergeIfPossible(curEdge)
      }
    }
    merges.values.toSet
  }
}
