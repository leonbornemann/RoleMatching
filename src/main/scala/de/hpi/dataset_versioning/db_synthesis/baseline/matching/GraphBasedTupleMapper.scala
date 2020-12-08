package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import scala.collection.mutable

class GraphBasedTupleMapper[A](edges: mutable.HashSet[General_1_to_1_TupleMatching[A]]) {

  val adjacencyList = mutable.HashMap[TupleReference[A],mutable.HashSet[TupleReference[A]]]()
  edges.foreach(e => {
    adjacencyList.getOrElseUpdate(e.tupleReferenceA,mutable.HashSet()) += e.tupleReferenceB
    adjacencyList.getOrElseUpdate(e.tupleReferenceB,mutable.HashSet()) += e.tupleReferenceA
  })
  val clusters = mutable.HashMap() ++ adjacencyList.keySet
    .map(k => (k,mutable.ArrayBuffer(k)))

  def canMergeClusters(edge: General_1_to_1_TupleMatching[A]): Boolean = {
    val elemsClusterA = clusters(edge.tupleReferenceA)
    val elemsClusterB = clusters(edge.tupleReferenceB)
    if(elemsClusterA==elemsClusterB){
      false
    } else {
      assert(elemsClusterA.intersect(elemsClusterB).size==0)
      if(elemsClusterA.size==1 && elemsClusterB.size==1)
        true
      else {
        var mergedClusterIsClique = true
        val clusterAList = elemsClusterA.toIndexedSeq
        val clusterBList = elemsClusterB.toIndexedSeq
        for(i <- 0 until clusterAList.size){
          val neighborsAElem = adjacencyList(clusterAList(i))
          for(j <- 0 until clusterBList.size){
            mergedClusterIsClique = neighborsAElem.contains(clusterBList(j))
            //TODO: code this with early abort!
          }
        }
        mergedClusterIsClique
      }
    }
  }

  def mergeClusters(edge: General_1_to_1_TupleMatching[A]) = {
    val elemsClusterA = clusters(edge.tupleReferenceA)
    val elemsClusterB = clusters(edge.tupleReferenceB)
    val mergedCluster = elemsClusterA ++ elemsClusterB
    clusters(edge.tupleReferenceA) = mergedCluster
    clusters(edge.tupleReferenceB) = mergedCluster
  }

  def mapGreedy() = {
    val edgesSorted = edges.toIndexedSeq
      .sortBy(_.score)
    edgesSorted.foreach(e => {
      if(canMergeClusters(e)){
        mergeClusters(e)
      }
    })
    clusters.values.map(v => General_Many_To_Many_TupleMatching(v.toSeq))
  }

}
