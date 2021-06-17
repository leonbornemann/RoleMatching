package de.hpi.tfm.evaluation.data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.fact_merging.optimization.SubGraph

import scala.util.Random

/***
 * Greedy optimization as in http://www.info.univ-angers.fr/pub/hao/papers/LuZhouHaoIEEETCyber2021.pdf (Algorithm 3)
 * @param c
 * @param seed
 */
class GreedyComponentOptimizer(c: SubGraph,log:Boolean) extends StrictLogging{

  def getEdgeWeight(v: c.graph.NodeT, y: Int):Double = {
    val edgeOption = v.incoming.find(_.nodes.exists(_.value==y))
    if(!edgeOption.isDefined)
      Double.NegativeInfinity
    else
      edgeOption.get.weight
  }

  def optimize() = {
    val cliqueCover = collection.mutable.HashSet[(collection.Set[Int],Double)]()
    var V = collection.mutable.HashSet() ++ c.graph.nodes
    while(!V.isEmpty){
      var objective = 0.0
      val Sk = collection.mutable.HashSet(V.head) // IMPORTANT: Algorithm says this should be random, but I don't think that matters here
      val candidates = collection.mutable.HashSet() ++ c.graph.find(Sk.head).get.neighbors.intersect(V)
      while(!candidates.isEmpty){
        val u = candidates.find(v => Sk.map(y => getEdgeWeight(v,y)).sum > 0)
        if(u.isDefined){
          objective += Sk.map(y => getEdgeWeight(u.get,y)).sum
          Sk.add(u.value.get)
          candidates.remove(u.get)
          candidates.union(u.get.neighbors.diff(Sk))
        } else {
          //no more remaining candidates - we are done!
          candidates.clear()
        }
      }
      V = V.diff(Sk)
      cliqueCover.add((Sk.map(_.value),objective))
    }
    if(log){
      logger.debug(s"Finished Greedy Algorithm for Component ${c.componentName} (${c.nVertices} vertices, ${c.nEdges} edges)")
      logger.debug(s"Found Cliques: ${cliqueCover.toIndexedSeq.sortBy(-_._2)}")
      logger.debug(s"Total objective: ${cliqueCover.map(_._2).sum}")
    }
    //validate result:
    cliqueCover.foreach{case (cc,w) => {
      assert(w>=0)
      val vertices = cc.toIndexedSeq
      for(i <- 0 until vertices.size){
        val v = vertices(i)
        for(j <- (i+1) until vertices.size){
          val w = vertices(j)
          assert(c.edgeExists(v,w))
        }
      }
    }}
    cliqueCover.map{case (cc,objective) => IdentifiedTupleMerge(cc,objective)}
  }

}
