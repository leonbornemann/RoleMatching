package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.sketches.field.AbstractTemporalField

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GraphBasedTupleMapper[A](vertices:IndexedSeq[TupleReference[A]],edges: mutable.HashSet[General_1_to_1_TupleMatching[A]]) {

  val adjacencyList = mutable.HashMap[TupleReference[A],mutable.HashSet[TupleReference[A]]]()
  edges.foreach(e => {
    adjacencyList.getOrElseUpdate(e.tupleReferenceA,mutable.HashSet()) += e.tupleReferenceB
    adjacencyList.getOrElseUpdate(e.tupleReferenceB,mutable.HashSet()) += e.tupleReferenceA
  })
  val clusters = mutable.HashMap() ++ vertices
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

  def getScore(clique: ArrayBuffer[TupleReference[A]]): Int = {
    if(clique.size==1)
      0
    else {
      val newInsertTime = clique.map(tr => tr.table.insertTime).minBy(_.toEpochDay)
      val afterMerge = AbstractTemporalField.mergeAll(clique.toSeq).countChanges(newInsertTime, GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD)
      val beforeMerge = clique.map(tr => tr.getDataTuple.head.countChanges(tr.table.insertTime, GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD)).sum
      assert(beforeMerge > afterMerge)
      beforeMerge - afterMerge
    }
  }

  def mapGreedy() = {
    val edgesSorted = edges.toIndexedSeq
      .sortBy(_.score)
    var i=0
    while(i<edgesSorted.size && edgesSorted(i).score>0){
      val e = edgesSorted(i)
      if(canMergeClusters(e)){
        mergeClusters(e)
      }
      i+=1
    }
    clusters.values.toSet
      .map((v:ArrayBuffer[TupleReference[A]]) => General_Many_To_Many_TupleMatching(v.toSeq,getScore(v)))
      .toIndexedSeq
  }

}
