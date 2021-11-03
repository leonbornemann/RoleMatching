package de.hpi.role_matching.cbrm.sgcp

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.compatibility_graph.representation.SubGraph

import scala.jdk.CollectionConverters.CollectionHasAsScala

class BruteForceComponentOptimizer(component: NewSubgraph) extends Optimizer(component) with StrictLogging{

  val verticesOrdered = component.graph.vertexSet().asScala.toIndexedSeq.sorted
  var allPartitions = collection.mutable.ArrayBuffer[collection.IndexedSeq[collection.IndexedSeq[Int]]]()


  def getAllPartitioningsOfSize2(vertices: collection.IndexedSeq[Int]) = {
    if(vertices.size<2)
      IndexedSeq()
    val partitionPairs = collection.mutable.ArrayBuffer[collection.IndexedSeq[collection.IndexedSeq[Int]]]()
    for(i <- 0 until Math.pow(2,vertices.size-1).toInt ){
      var bitMask = i
      val curFirstPartition = collection.mutable.ArrayBuffer[Int](vertices(0))
      val curSecondPartition = collection.mutable.ArrayBuffer[Int]()
      var j=1
      while (j < vertices.size) {
        if ((bitMask & 1) == 0) { // Calculate sum
          //first partition
          curFirstPartition.append(vertices(j))
        } else {
          //second partition
          curSecondPartition.append(vertices(j))
        }
        bitMask = bitMask >> 1
        j+=1
      }
      assert(j==vertices.size)
      partitionPairs.append(IndexedSeq(curFirstPartition,curSecondPartition))
    }
    partitionPairs
  }


  def addPartition(p: collection.IndexedSeq[collection.IndexedSeq[Int]]) = {
    allPartitions.append(p)
  }

  def getAllPartitionings(vertices:collection.IndexedSeq[Int]):collection.IndexedSeq[collection.IndexedSeq[collection.IndexedSeq[Int]]] = {
    if(vertices.size==0)
      IndexedSeq()
    else if(vertices.size==1)
      IndexedSeq(IndexedSeq(vertices))
    else {
      val partitionPairs = getAllPartitioningsOfSize2(vertices)
//      partitionPairs.foreach(p => addPartition(p))
      val withSecondOneSplit = partitionPairs
        .withFilter(pair => pair(1).size>1)
        .flatMap(pair => {
          assert(pair.size == 2)
          val secondDivided = getAllPartitionings(pair(1))
          val res = secondDivided
            .map(dvd => {
              //assert(dvd.size == 2)
              val res = IndexedSeq(pair(0)) ++ dvd
//              if(res.exists(_.size==4))
//                println()
              res
            })
          res
        //getAllPartitionings(pair(0)) ++ getAllPartitionings(pair(1))
      })
      val result = partitionPairs ++ withSecondOneSplit
//      if(result.exists(_.exists(_.size==4)))
//        println()
      result
    }
  }

  def printPartitions(allPartitionings: collection.IndexedSeq[collection.IndexedSeq[collection.IndexedSeq[Int]]]) = {
    allPartitionings.foreach(partitioning => {
      val asString = "{" +partitioning.map(partition => "{" + partition.mkString(",") +"}").mkString(" , ") + "}"
      println(asString)
    })
    println(s"Total Num Partitions: ${allPartitionings.size}")
  }

  def getScore(partitioning: collection.IndexedSeq[collection.IndexedSeq[Int]]) = {
    partitioning.map(p => {
      val score = getCliqueScore(p)
      score
    }).sum
  }

  def optimize() = {
    val allPartitionings = getAllPartitionings(verticesOrdered)
    if(allPartitionings.size==0){
      logger.debug("What?")
      logger.debug(s"Component:${component.componentName},Vertices: ${component.nVertices},Edges: ${component.nEdges}")
    }
    assert(allPartitionings.size>0)
    val sortedList = allPartitionings
      .map(partitioning => (partitioning.filter(_.size > 0), getScore(partitioning)))
      .sortBy(e => (-e._2,-e._1.size))
    val (best,score) = sortedList.head
    best.map(vertices => RoleMerge(vertices.toSet,getCliqueScore(vertices)))
  }

}
