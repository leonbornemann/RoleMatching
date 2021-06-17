package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.evaluation.data.ConnectedComponentBasedOptimizationMain.componentDir
import de.hpi.tfm.evaluation.data.{ComponentWiseOptimizer, GreedyComponentOptimizer, IdentifiedTupleMerge}
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.File

class HybridOptimizer(graph: Graph[Int, WUnDiEdge],
                      resultFile:File) extends ComponentWiseOptimizer(graph,resultFile) {
  def componentIterator() = new ComponentIterator(graph)

  override def optimizeComponent(component: SubGraph): Iterable[IdentifiedTupleMerge] = {
    val name = component.componentName
    if(component.nVertices<8){
      //we can do brute-force easily enough
      new GreedyComponentOptimizer(component,true).optimize()
    } else if(component.nVertices>=8 && component.nVertices<500){
      //use related work MDMCP approach
      component.toMDMCPInputFile(new File(componentDir + s"/$name.txt"))
      new GreedyComponentOptimizer(component,true).optimize()
    } else {
      new GreedyComponentOptimizer(component,true).optimize()
    }
  }
}
