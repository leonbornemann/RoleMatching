package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.sketches.field.AbstractTemporalField

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GraphBasedTupleMapper[A](vertices:IndexedSeq[TupleReference[A]],edges: mutable.HashSet[General_1_to_1_TupleMatching[A]]) {

  val adjacencyList = mutable.HashMap[TupleReference[A],mutable.HashMap[TupleReference[A],Int]]()
  edges.foreach(e => {
    adjacencyList.getOrElseUpdate(e.tupleReferenceA,mutable.HashMap()).put(e.tupleReferenceB,e.evidence)
    adjacencyList.getOrElseUpdate(e.tupleReferenceB,mutable.HashMap()).put(e.tupleReferenceA,e.evidence)
  })
  val clusters = mutable.HashMap() ++ vertices
    .map(k => (k,(mutable.ArrayBuffer(k),0)))

  def tryMergeClusters(edge: General_1_to_1_TupleMatching[A]): Boolean = {
    assert(edge.evidence>0)
    val (elemsClusterA,evidenceCluster1) = clusters(edge.tupleReferenceA)
    val (elemsClusterB,evidenceCluster2) = clusters(edge.tupleReferenceB)
    if(elemsClusterA==elemsClusterB){
      //is already mege
      false
    } else {
      assert(elemsClusterA.intersect(elemsClusterB).size==0)
      var mergedClusterIsClique = true
      val clusterAList = elemsClusterA.toIndexedSeq
      val clusterBList = elemsClusterB.toIndexedSeq
      var interClusterEvidenceSum = 0
      for(i <- 0 until clusterAList.size){
        val neighborsAElem = adjacencyList(clusterAList(i))
        for(j <- 0 until clusterBList.size){
          val edge = neighborsAElem.get(clusterBList(j))
          if(edge.isDefined){
            interClusterEvidenceSum +=edge.get
          } else{
            mergedClusterIsClique = false
          }
          //TODO: code this with early abort if it is a runtime problem!
        }
      }
      if(mergedClusterIsClique) {
        val mergedCluster = (elemsClusterA ++ elemsClusterB,evidenceCluster1+evidenceCluster2+interClusterEvidenceSum)
        clusters(edge.tupleReferenceA) = mergedCluster
        clusters(edge.tupleReferenceB) = mergedCluster
        true
      } else {
        false
      }
    }
  }

  def getChangeScore(clique: ArrayBuffer[TupleReference[A]]): (Int,Int) = {
    if(clique.size==1)
      clique.head.getDataTuple.head.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)
    else {
      val afterMerge = AbstractTemporalField.mergeAll(clique.toSeq).countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)
      val beforeMerge = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.sumChangeRanges(clique.map(tr => tr.getDataTuple.head.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)))
      if(beforeMerge._1<=afterMerge._1){
        clique.foreach(println(_))
        println(afterMerge)
        println(beforeMerge)
      }
      assert(beforeMerge._1 > afterMerge._1)
      afterMerge
    }
  }

  def mapGreedy() = {
    val edgesSorted = edges.toIndexedSeq
      .sortBy(- _.evidence)
    var i=0
    while(i<edgesSorted.size && edgesSorted(i).evidence>0){
      val e = edgesSorted(i)
      tryMergeClusters(e)
      i+=1
    }
    clusters.values.toSet
      .map( (t:(ArrayBuffer[TupleReference[A]],Int)) => {
        val totalEvidence:Int = t._2
        val trs = t._1
        General_Many_To_Many_TupleMatching(trs.toSeq,totalEvidence,getChangeScore(trs))
      })
      //.map{case (v:ArrayBuffer[TupleReference[A]],totalEvidence:Int) => General_Many_To_Many_TupleMatching(v.toSeq,totalEvidence,getChangeScore(v))}
      .toIndexedSeq

  }

}
