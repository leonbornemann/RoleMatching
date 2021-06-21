package de.hpi.tfm.data.wikipedia.infobox.evaluation

import de.hpi.tfm.data.wikipedia.infobox.evaluation.CliqueBasedEvaluationGreedyVSMDMCP.resultFile
import de.hpi.tfm.data.wikipedia.infobox.fact_merging.VerticesOrdered
import de.hpi.tfm.evaluation.data.IdentifiedTupleMerge

import java.io.PrintWriter

case class CliqueAnalyser(pr: PrintWriter,verticesOrdered: VerticesOrdered) {

  def serializeSchema() = {
    pr.println("ComponentID,Method,cliqueID,cliqueSize,remainsValidPercentage,score") //cliqueID is specific per method
  }

  def addResultTuple(c: IdentifiedTupleMerge,componentID:String, method: String) = {
    val verticesSorted = c.clique.toIndexedSeq.sorted
    val cliqueID = verticesSorted.head
    val vertices = c.clique.map(i => verticesOrdered.vertices(i)).toIndexedSeq
    var validEdges = 0
    var edgesTotal = 0
    for(i <- 0 until vertices.size){
      for(j <- (i+1) until vertices.size){
        edgesTotal+=1
        val l1 = vertices(i).factLineage.toFactLineage
        val l2 = vertices(j).factLineage.toFactLineage
        if(l1.tryMergeWithConsistent(l2).isDefined)
          validEdges+=1
      }
    }
    val remainsValidPercentage = validEdges / edgesTotal.toDouble
    pr.println(s"$componentID,$method,$cliqueID,${c.clique.size},$remainsValidPercentage,${c.cliqueScore}")
  }

  def addResultTuples(cliquesGreedy: collection.Seq[IdentifiedTupleMerge],componentID:String, method: String) ={
    cliquesGreedy.foreach(c => addResultTuple(c,componentID,method))
  }


}
