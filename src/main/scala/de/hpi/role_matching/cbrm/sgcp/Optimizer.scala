package de.hpi.role_matching.cbrm.sgcp

abstract class Optimizer(c: NewSubgraph) {

  def getEdgeWeight(v:Int,w:Int):Double = {
    val edge = c.graph.getEdge(v,w)
    if(edge==null)
      Double.NegativeInfinity
    else
      c.graph.getEdgeWeight(edge)
  }

  def getCliqueScore(vertices:collection.IndexedSeq[Int]) = {
    var i=0
    var invalid = false
    var score = 0.0
    while(i<vertices.size && !invalid){
      val v = vertices(i)
      var j = i+1
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
