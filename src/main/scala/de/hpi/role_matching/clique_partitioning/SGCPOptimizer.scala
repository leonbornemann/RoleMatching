package de.hpi.role_matching.clique_partitioning

import de.hpi.role_matching.clique_partitioning.asad.SmartLargeComponentOptimizer
import de.hpi.role_matching.compatibility.graph.representation.SubGraph
import scalax.collection.Graph
import scalax.collection.edge.WUnDiEdge

import java.io.{File, PrintWriter}

class SGCPOptimizer(graph: Graph[Int, WUnDiEdge],
                    resultDir:File,
                    mdmcpExportDir:File,
                    //vertexLookupDirForPartitions:File,
                    greedyMergeDir:File,
                    useGreedyOnly:Boolean=false
                     ) extends ComponentWiseOptimizer(graph,resultDir) {

  val prBruteForce = new PrintWriter(s"${resultDir.getAbsolutePath}/bruteForceResult.json")
  val prGreedyLargeVertexCount = new PrintWriter(s"${resultDir.getAbsolutePath}/greedyLargeVertexCountResult.json")
  val prSingleVertexComponents = new PrintWriter(s"${resultDir.getAbsolutePath}/sinlgeVertexComponents.json")

  def componentIterator() = new ComponentIterator(graph)

  greedyMergeDir.mkdir()

  def serializeMerges(merges: collection.Iterable[RoleMerge], pr: PrintWriter) = {
    merges.foreach(tm => {
      tm.appendToWriter(pr,false,true)
    })
  }

  def checkMergeIntegrity(merges: collection.Iterable[RoleMerge], component: SubGraph) = {
    assert(merges.toIndexedSeq.flatMap(_.clique).size==component.nVertices)
  }

  override def optimizeComponent(component: SubGraph) = {
    val name = component.componentName
    //new File("debug_components/").mkdir()
//    if(component.componentName==31408){
//      component.toSerializableComponent.toJsonFile(new File(s"debug_components/$name.json"))
//    }
    if(useGreedyOnly){
      val merges = new GreedyComponentOptimizer(component,true).optimize()
      serializeMerges(merges,prGreedyLargeVertexCount)
      checkMergeIntegrity(merges,component)
    } else {
      if(component.nVertices==1){
        RoleMerge(Set(component.graph.nodes.head.value),0.0).appendToWriter(prSingleVertexComponents,false,true)
      } else if(component.nVertices<8){
        //we can do brute-force easily enough
        val merges = new BruteForceComponentOptimizer(component).optimize()
        serializeMerges(merges,prBruteForce)
        checkMergeIntegrity(merges,component)
      } else if(component.nVertices>=8 && component.nVertices<500){
        //      component.toSerializableComponent.toJsonFile(new File(s"debug_components/$name.json"))
        //      logger.debug(s"Handling Component s$name")
        //use related work MDMCP approach
        component.toMDMCPInputFile(new File(mdmcpExportDir.getAbsolutePath + s"/$name.txt"))
        //For debug purposes:      component.writePartitionVertexFile(new File(vertexLookupDirForPartitions.getAbsolutePath +  s"/$name.txt"))
        //      val greedyRes = new GreedyComponentOptimizer(component,true).optimize()
        //      val greedyFileForComponent = new File(greedyMergeDir.getAbsolutePath + s"/$name.json")
        //      val pr = new PrintWriter(greedyFileForComponent)
        //      greedyRes.foreach(_.appendToWriter(pr,false,true))
        //      pr.close()
        //      greedyRes
      } else {
        //      logger.debug(s"Skipping component with ${component.nVertices} vertices")
        val merges = new GreedyComponentOptimizer(component,true).optimize()
        //val newOptimizer = new SmartLargeComponentOptimizer(component)
        serializeMerges(merges,prGreedyLargeVertexCount)
        checkMergeIntegrity(merges,component)
      }
    }
  }

  override def closeAllWriters(): Unit = {
    prBruteForce.close()
    prGreedyLargeVertexCount.close()
    prSingleVertexComponents.close()
  }
}
