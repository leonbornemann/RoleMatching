package de.hpi.role_matching.evaluation.clique

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.clique_partitioning.{RoleMerge, ScoreConfig}
import de.hpi.role_matching.compatibility.graph.representation.simple.GeneralEdge
import de.hpi.role_matching.compatibility.graph.representation.slim.VertexLookupMap
import de.hpi.role_matching.compatibility.graph.representation.vertex.VerticesOrdered
import de.hpi.role_matching.evaluation.StatComputer
import de.hpi.role_matching.evaluation.edge.NewEdgeStatRow

import java.io.PrintWriter
import java.time.LocalDate

case class CliqueAnalyser(prCliques: PrintWriter,
                          prEdges:PrintWriter,
                          vertexLookupMap: VertexLookupMap,
                          trainTimeEnd: LocalDate,
                          scoreConfig: ScoreConfig) extends StatComputer {

  def serializeSchema() = {
    prCliques.println("ComponentID,Method,cliqueID,cliqueSize,edgesTotal,validEdges,totalEvidence,fractionOfVerticesWithEvidence,score,alpha") //cliqueID is specific per method
    prEdges.println("ComponentID,Method,cliqueID,cliqueSize,vertex1ID,vertex2ID,remainsValid,evidence,score")
  }

  def addResultTuple(c: RoleMerge, componentID: String, method: String) = {
    val verticesSorted = c.clique.toIndexedSeq.sorted
    val cliqueID = verticesSorted.head
    val vertices = c.clique.map(i => vertexLookupMap.posToFactLineage(i)).toIndexedSeq
    val identifiedVertices = c.clique.map(i => vertexLookupMap.posToLineage(i)).toIndexedSeq
    var evidenceCountTotal = 0
    var validEdges = 0
    var edgesTotal = 0
    val hasEvidence = collection.mutable.HashMap[Int, Boolean]()
    for (i <- 0 until vertices.size) {
      for (j <- (i + 1) until vertices.size) {
        edgesTotal += 1
        val l1 = vertices(i)
        val l2 = vertices(j)
        val evidenceInThisEdge = getEvidenceInTestPhase(l1, l2, trainTimeEnd)
        evidenceCountTotal += evidenceInThisEdge
        if (evidenceInThisEdge > 0) {
          hasEvidence.put(i, true)
          hasEvidence.put(j, true)
        }
        var remainsValid = false
        if (l1.tryMergeWithConsistent(l2).isDefined) {
          validEdges += 1
          remainsValid=true
        }
        prEdges.println(s"$componentID,$method,$cliqueID,${c.clique.size},$i,$j,$remainsValid,$evidenceInThisEdge") //TODO: compute score?
      }
    }
    val verticesWithAtLeastOneEdgeWithEvidence = hasEvidence.values.filter(identity).size
    val fractionOfVerticesWithEvidence = verticesWithAtLeastOneEdgeWithEvidence / vertices.size.toDouble
    prCliques.println(s"$componentID,$method,$cliqueID,${c.clique.size},$edgesTotal,$validEdges,$evidenceCountTotal,$fractionOfVerticesWithEvidence,${c.cliqueScore},${scoreConfig.alpha}")
  }

  def addResultTuples(cliquesGreedy: collection.Seq[RoleMerge], componentID: String, method: String) = {
    cliquesGreedy.foreach(c => addResultTuple(c, componentID, method))
  }


}
