package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.compatibility.graph.fact.{FactMergeabilityGraphEdge, TupleReference}
import de.hpi.tfm.evaluation.data.IdentifiedTupleMerge
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

class GreedyEdgeWeightOptimizerForComponent(val subGraph: Graph[String, WUnDiEdge],MIN_EDGE_WEIGHT_THRESHOLD:Double) {

  val vertexPairToEdge = subGraph.edges.map(e => {
    val vertexSet = e.nodes.map(_.value).toSet
    (vertexSet,e)
    })
    .toMap

  val merges = scala.collection.mutable.HashMap() ++ subGraph.nodes
    .map(n => (n.value,IdentifiedTupleMerge(Set(n.value),MIN_EDGE_WEIGHT_THRESHOLD))) //every node starts out with the min threshold originally, so that we only merge if we exceed that
    .toMap

  def executeMergeIfPossibleA(curEdge: FactMergeabilityGraphEdge) = {

  }

  def vertexPairFromEdge(curEdgeObject: subGraph.EdgeT) = {
    val seq = curEdgeObject.nodes.toIndexedSeq
    assert(seq.size==2)
    (seq(0).value,seq(1).value)
  }

  def executeMergeIfPossible(curEdgeObject: subGraph.EdgeT) = {
    val (v1,v2) = vertexPairFromEdge(curEdgeObject)
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
        val edge = vertexPairToEdge.get(Set(n1,n2))
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
        val newMerge = IdentifiedTupleMerge(mergedClique,merges(v1).cliqueScore + merges(v2).cliqueScore + newScore)
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
      if(curEdgeObject.weight<=MIN_EDGE_WEIGHT_THRESHOLD){
        done = true
      } else {
        executeMergeIfPossible(curEdgeObject)
      }
    }
    merges.values.toSet
  }
}
