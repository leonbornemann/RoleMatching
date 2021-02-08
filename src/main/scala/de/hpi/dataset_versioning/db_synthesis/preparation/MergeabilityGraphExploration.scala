package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object MergeabilityGraphExploration extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val graph = FieldLineageMergeabilityGraph.readTableGraph(subdomain)
    .sortBy(-_._2)
  val associationMergeabilityGraph = AssociationMergeabilityGraph(graph.map{case (associations,summedEvidence) => {
    assert(associations.size == 2)
    val list = associations.toList
    AssociationMergeabilityGraphEdge(list(0), list(1), summedEvidence)
  }
  })
  associationMergeabilityGraph.writeToStandardFile(subdomain)
  val graphRead = AssociationMergeabilityGraph.readFromStandardFile(subdomain)
//  graph.take(500)
//    .foreach(t => println(t))
//  println("Last ----------------------------------------------------------------------")
//  graph.takeRight(500)
//    .foreach(t => println(t))
//  println("Size:---------------------------------------------------------")
//  println(graph.size)
  //connected components:
  val asMap = graphRead.edges.groupBy(e => Set(e.v1,e.v2))
  val components = asMap.keySet.flatMap(k => k.map(id => (id,scala.collection.mutable.HashSet(id))))
    .toMap
  assert(components.forall(t => t._1==t._2.head && t._2.size==1))
  asMap.foreach(e =>{
    assert(e._1.size==2)
    val asList= e._1.toList
    components(asList(0)) ++= components(asList(1))
    components(asList(1)) ++= components(asList(0))
  })
  val byConnectedComponent = components.groupMap(_._2)(_._1)
  byConnectedComponent.foreach{case (k,v) => {
    if(k!=v.toSet){
      println(k)
      println(v.toSet)
    }
    assert(k==v.toSet) //TODO: this fails!
  }}
  val connectedComponents = byConnectedComponent.keySet
  //histogram the size:
  println("VertexCount,NumComponents")
  connectedComponents.groupBy(_.size)
    .map{case (size,members) => (size,members.size)}
    .toIndexedSeq
    .sortBy(_._1)
    .foreach(t => println(s"${t._1},${t._2}"))


}
