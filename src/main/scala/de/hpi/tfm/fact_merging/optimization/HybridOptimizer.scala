package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.evaluation.data.{ComponentWiseOptimizer, GreedyComponentOptimizer, IdentifiedTupleMerge}
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.{File, PrintWriter}

class HybridOptimizer(graph: Graph[Int, WUnDiEdge],
                      resultFile:File,
                      mdmcpExportDir:File,
                      vertexLookupDirForPartitions:File,
                      greedyMergeDir:File
                     ) extends ComponentWiseOptimizer(graph,resultFile) {
  def componentIterator() = new ComponentIterator(graph)

  greedyMergeDir.mkdir()

  override def optimizeComponent(component: SubGraph): Iterable[IdentifiedTupleMerge] = {
    val name = component.componentName
    if(component.nVertices<8){
      //we can do brute-force easily enough
      //skipping this
      logger.debug(s"Skipping component with ${component.nVertices} vertices")
      //new GreedyComponentOptimizer(component,true).optimize()
      component.graph.nodes.map(n => IdentifiedTupleMerge(Set(n.value),0.0))
    } else if(component.nVertices>=8 && component.nVertices<500){
      //use related work MDMCP approach
      component.toMDMCPInputFile(new File(mdmcpExportDir.getAbsolutePath + s"/$name.txt"))
      component.writePartitionVertexFile(new File(vertexLookupDirForPartitions.getAbsolutePath +  s"/$name.txt"))
      val greedyRes = new GreedyComponentOptimizer(component,true).optimize()
      val greedyFileForComponent = new File(greedyMergeDir.getAbsolutePath + s"/$name.json")
      val pr = new PrintWriter(greedyFileForComponent)
      greedyRes.foreach(_.appendToWriter(pr,false,true))
      pr.close()
      greedyRes
    } else {
      logger.debug(s"Skipping component with ${component.nVertices} vertices")
      //new GreedyComponentOptimizer(component,true).optimize()
      component.graph.nodes.map(n => IdentifiedTupleMerge(Set(n.value),0.0))
    }
  }
}
