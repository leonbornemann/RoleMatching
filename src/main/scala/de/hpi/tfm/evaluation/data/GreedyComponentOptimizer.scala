package de.hpi.tfm.evaluation.data

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.fact_merging.optimization.SubGraph

import scala.util.Random

/***
 * Greedy optimization as in http://www.info.univ-angers.fr/pub/hao/papers/LuZhouHaoIEEETCyber2021.pdf (Algorithm 3)
 * @param c
 * @param seed
 */
class GreedyComponentOptimizer(c: SubGraph,log:Boolean) extends Optimizer(c) with StrictLogging {

  def optimize() = {
    val cliqueCover = collection.mutable.HashSet[(collection.Set[Int],Double)]()
    var V = collection.mutable.HashSet() ++ c.graph.nodes
    while(!V.isEmpty){
      var objective = 0.0
      val curClique = collection.mutable.HashSet(V.head) // IMPORTANT: Algorithm says this should be random, but I don't think that matters here
      val candidates = collection.mutable.HashSet() ++ c.graph.find(curClique.head).get.neighbors.intersect(V)
      val edgesCovered = scala.collection.mutable.ArrayBuffer[(Int,Int,Double)]()
      while(!candidates.isEmpty){
        val u = candidates.find(v => curClique.map(y => getEdgeWeight(v,y)).sum > 0)
        if(u.isDefined){
          objective += curClique.toIndexedSeq.map(y => getEdgeWeight(u.get,y)).sum
          curClique.add(u.value.get)
          candidates.remove(u.get)
          candidates.union(u.get.neighbors.diff(curClique))
        } else {
          //no more remaining candidates - we are done!
          candidates.clear()
        }
      }
      V = V.diff(curClique)
      cliqueCover.add((curClique.map(_.value),objective))
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
      var newWeight = 0.0
      for(i <- 0 until vertices.size){
        val v = vertices(i)
        for(j <- (i+1) until vertices.size){
          val w = vertices(j)
          assert(c.getEdgeWeight(v,w)==getEdgeWeight(v,w))
          newWeight += c.getEdgeWeight(v,w)
          assert(c.edgeExists(v,w))
        }
      }
      if(!(Math.abs(w - newWeight) < 0.0000001)){
        logger.debug("Bug found in Greedy:")
        logger.debug(s"Clique: ${cc.min} ")
        logger.debug(s"Weight by greedy: ${w} ")
        logger.debug(s"Weight from edges: ${newWeight} ")
      }
      assert(Math.abs(w - newWeight) < 0.0000001)
    }}
    cliqueCover.map{case (cc,objective) => IdentifiedTupleMerge(cc,objective)}
  }

}
