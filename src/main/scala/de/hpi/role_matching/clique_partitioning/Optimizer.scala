package de.hpi.role_matching.clique_partitioning

import de.hpi.role_matching.compatibility.graph.representation.SubGraph

abstract class Optimizer(c: SubGraph) {

  def getEdgeWeight(v: c.graph.NodeT, y: Int):Double = {
    val edgeOption = v.incoming.find(_.nodes.exists(_.value==y))
    if(!edgeOption.isDefined)
      Double.NegativeInfinity
    else
      edgeOption.get.weight
  }

  def getEdgeWeight(v:Int,w:Int):Double = {
    val edgeOption = c.graph.find(v).get.incoming.find(_.nodes.exists(_.value==w))
    if(!edgeOption.isDefined)
      Double.NegativeInfinity
    else
      edgeOption.get.weight
  }

  def getCliqueScore(vertices:collection.IndexedSeq[Int]) = {
    var i=0
    var invalid = false
    var score = 0.0
    while(i<vertices.size && !invalid){
      val v = vertices(i)
      var j = 0+1
      while(j<vertices.size && !invalid) {
        val w = vertices(j)
        val weight = getEdgeWeight(v,w)
        if(weight==Double.NegativeInfinity) {
          score=Double.NegativeInfinity
          invalid=true
        } else
          score+=weight
        j+=1
      }
      i+=1
    }
    score
  }



}
