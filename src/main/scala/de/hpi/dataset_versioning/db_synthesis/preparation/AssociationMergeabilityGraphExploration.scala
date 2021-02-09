package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.io.IOService

class AssociationMergeabilityGraphExploration extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val graphRead = AssociationMergeabilityGraph.readFromStandardFile(subdomain)
  val graph = graphRead.toScalaGraph
  println(graph.nodes.size)
  println(graph.edges.size)
  val traverser = graph.componentTraverser()
  val sizes = traverser.map(c => {
    println(c.nodes.size,c.edges.map(_.weight).sum)
    c.nodes.size
  })
  sizes.groupBy(identity)
    .map{case (k,v) => (k,v.size)}
    .toIndexedSeq
    .sortBy(_._1)
    .foreach(println(_))
  println("--------------------------------------------------------------------------------------------------")
  //connected components:
  val asMap = graphRead.edges.groupBy(e => Set(e.v1,e.v2))
  val components = scala.collection.mutable.HashMap() ++ asMap.keySet.flatMap(k => k.map(id => (id,scala.collection.mutable.HashSet(id))))
    .toMap
  assert(components.forall(t => t._1==t._2.head && t._2.size==1))
  asMap.foreach(e =>{
    assert(e._1.size==2)
    val asList= e._1.toList
    val firstConnectedComponent = components(asList(0))
    val secondConnectedComponent = components(asList(1))
    firstConnectedComponent ++= secondConnectedComponent
    //update pointers of all members of second component to first component
    secondConnectedComponent.foreach(dtt => components(dtt) = firstConnectedComponent)
  })
  val byConnectedComponent = components.groupMap(_._2)(_._1)
  //integrity check: all connected components
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
