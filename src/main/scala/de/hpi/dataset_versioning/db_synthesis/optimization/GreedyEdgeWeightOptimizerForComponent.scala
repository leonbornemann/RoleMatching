package de.hpi.dataset_versioning.db_synthesis.optimization

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference
import de.hpi.dataset_versioning.db_synthesis.graph.field_lineage.FieldLineageGraphEdge
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

import scala.collection.mutable

class GreedyEdgeWeightOptimizerForComponent(val subGraph: Graph[TupleReference[Any], WLkUnDiEdge]) {

  private var MIN_EDGE_WEIGHT_THRESHOLD = 0.0

  val adjacencyList = subGraph.edges.map(e => {
    val edgeObject = e.label.asInstanceOf[FieldLineageGraphEdge]
    (Set(edgeObject.tupleReferenceA,edgeObject.tupleReferenceB),e)
    })
    .toMap

  val merges = scala.collection.mutable.HashMap() ++ subGraph.nodes
    .map(n => (n.value.toIDBasedTupleReference,TupleMerge(Set(n.value.toIDBasedTupleReference),0.0)))
    .toMap

  def executeMergeIfPossible(curEdge: FieldLineageGraphEdge) = {
    val v1 = curEdge.tupleReferenceA
    val v2 = curEdge.tupleReferenceB
    val curV1Clique = merges(v1).clique
    val curV2Clique = merges(v2).clique
    if(curV1Clique==curV2Clique){
      //do nothing
    } else {
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
        merges(v1) = newMerge
        merges(v2) = newMerge
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
      val curEdge = curEdgeObject.label.asInstanceOf[FieldLineageGraphEdge]
      if(curEdgeObject.weight<=MIN_EDGE_WEIGHT_THRESHOLD){
        done = true
      } else {
        executeMergeIfPossible(curEdge)
      }
    }
    merges.values.toSet
  }
}
