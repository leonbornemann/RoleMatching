package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//adjacency list only contains every edge once (from the lower index to the higher index)! First element of adjacency list is the node index, second is the score
case class MDMCPInputGraph(verticesOrdered: IndexedSeq[String], adjacencyList: collection.Map[Int, collection.Map[Int, Int]]) extends JsonWritable[MDMCPInputGraph]{

  //serializes this as an adjacencyMatrix file
  def serializeToMDMCPInputFile(f:File) = {
    val pr = new PrintWriter(f)
    pr.println(s" ${verticesOrdered.size}")
    verticesOrdered
      .zipWithIndex
      .foreach{case (_,i) => {
        val neighbors = adjacencyList.getOrElse(i,Map[Int,Int]())
        val weights = (i until verticesOrdered.size).map{ j =>
          val weight = neighbors.getOrElse(j,Integer.MIN_VALUE)
          weight
        }
        pr.println(weights.mkString("  "))
      }}
    pr.close()
  }

}
object MDMCPInputGraph extends JsonReadable[MDMCPInputGraph]
