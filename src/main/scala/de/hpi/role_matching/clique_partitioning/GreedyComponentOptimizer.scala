package de.hpi.role_matching.clique_partitioning

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.compatibility.graph.representation.SubGraph

import scala.jdk.CollectionConverters.CollectionHasAsScala

/***
 * Greedy optimization as in http://www.info.univ-angers.fr/pub/hao/papers/LuZhouHaoIEEETCyber2021.pdf (Algorithm 3)
 * @param c
 * @param seed
 */
class GreedyComponentOptimizer(c: NewSubgraph,log:Boolean) extends Optimizer(c) with StrictLogging {

  def optimize() = {
    val cliqueCover = collection.mutable.HashSet[(collection.Set[Int],Double)]()
    var V = collection.mutable.HashSet() ++ c.graph.vertexSet().asScala
    while(!V.isEmpty){
      var objective = 0.0
      val curClique = collection.mutable.HashSet(V.head) // IMPORTANT: Algorithm says this should be random, but I don't think that matters here
      val candidates = collection.mutable.HashSet() ++ c.neighborsOf(curClique.head).intersect(V)
      val edgesCovered = scala.collection.mutable.ArrayBuffer[(Int,Int,Double)]()
      while(!candidates.isEmpty){
        //val u = candidates.find(v => curClique.map(y => getEdgeWeight(v,y)).sum > 0)
        val withGain = candidates.toIndexedSeq.map(v => (v,curClique.map(y => getEdgeWeight(v,y)).sum))
        val (u,gain) = withGain.sortBy(-_._2).head
        if(gain>0){
          objective += gain
          curClique.add(u)
          candidates.remove(u)
          candidates.union(c.neighborsOf(u).diff(curClique))
        } else {
          //no more remaining candidates - we are done!
          candidates.clear()
        }
      }
      V = V.diff(curClique)
      cliqueCover.add((curClique,objective))
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
          assert(getEdgeWeight(v,w)==getEdgeWeight(v,w))
          newWeight += getEdgeWeight(v,w)
          assert(c.graph.containsEdge(v,w))
        }
      }
      if(!(Math.abs(w - newWeight) < 0.0000001)){
        logger.debug(s"Bug found in Greedy: Clique: ${cc.min}, size: ${cc.size}, Weight by greedy: ${w} Weight from edges: ${newWeight}  ")
      }
      //assert(Math.abs(w - newWeight) < 0.0000001)
      assert(newWeight>0)
    }}
    cliqueCover.map{case (cc,objective) => {
      RoleMerge(cc,objective)
    }}
  }

}
