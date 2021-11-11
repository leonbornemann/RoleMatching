package de.hpi.role_matching.cbrm.sgcp

import org.jgrapht.Graph
import org.jgrapht.graph.DefaultWeightedEdge

import java.io.{File, PrintWriter}

class SGCPOptimizer(graph: Graph[Int, DefaultWeightedEdge],
                    resultDir:File,
                    mdmcpExportDir:File,
                    vertexLookupDirForPartitions:File) extends ComponentWiseOptimizer(graph,resultDir) {

  val prBruteForce = new PrintWriter(s"${resultDir.getAbsolutePath}/bruteForceResult.json")
  val prGreedyLargeVertexCount = new PrintWriter(s"${resultDir.getAbsolutePath}/greedyLargeVertexCountResult.json")
  val prSingleVertexComponents = new PrintWriter(s"${resultDir.getAbsolutePath}/sinlgeVertexComponents.json")

  def serializeMerges(merges: collection.Iterable[RoleMerge], pr: PrintWriter) = {
    merges.foreach(tm => {
      tm.appendToWriter(pr,false,true)
    })
  }

  def checkMergeIntegrity(merges: collection.Iterable[RoleMerge], component: NewSubgraph) = {
    assert(merges.toIndexedSeq.flatMap(_.clique).size==component.nVertices)
  }

  override def optimizeComponent(component:NewSubgraph) = {
    val name = component.componentName
    //new File("debug_components/").mkdir()
//    if(component.componentName==31408){
//      component.toSerializableComponent.toJsonFile(new File(s"debug_components/$name.json"))
//    }

    if(component.nVertices==1){
      RoleMerge(Set(component.graph.vertexSet().iterator.next()),0.0).appendToWriter(prSingleVertexComponents,false,true)
    } else if(component.nVertices<8){
      //we can do brute-force easily enough
      val merges = new BruteForceComponentOptimizer(component).optimize()
      serializeMerges(merges,prBruteForce)
      checkMergeIntegrity(merges,component)
    } else if(component.nVertices>=8 && component.nVertices<500){
      //use related work MDMCP approach
      component.toMDMCPInputFile(new File(mdmcpExportDir.getAbsolutePath + s"/$name.txt"))
      component.writePartitionVertexFile(new File(vertexLookupDirForPartitions.getAbsolutePath +  s"/$name.txt"))
    } else if(component.nVertices>=500){
      val merges = new GreedyComponentOptimizer(component,false).optimize()
      serializeMerges(merges,prGreedyLargeVertexCount)
      checkMergeIntegrity(merges,component)
    } else {
      println(component.nVertices)
      //should never get here
      logger.debug("Error in switch-case - we should never get here")
      assert(false)
    }

  }

  override def closeAllWriters(): Unit = {
    prBruteForce.close()
    prGreedyLargeVertexCount.close()
    prSingleVertexComponents.close()
  }
}
