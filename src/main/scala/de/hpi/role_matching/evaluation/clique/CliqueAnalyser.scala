package de.hpi.role_matching.evaluation.clique

import de.hpi.role_matching.clique_partitioning.RoleMerge
import de.hpi.role_matching.compatibility.graph.representation.vertex.VerticesOrdered
import de.hpi.role_matching.evaluation.StatComputer

import java.io.PrintWriter
import java.time.LocalDate

case class CliqueAnalyser(pr: PrintWriter, verticesOrdered: VerticesOrdered, trainTimeEnd: LocalDate, alpha: Float) extends StatComputer {

  def serializeSchema() = {
    pr.println("ComponentID,Method,cliqueID,cliqueSize,remainsValidPercentage,avgEvidencePerEdge,fractionOfVerticesWithEvidence,score,alpha") //cliqueID is specific per method
  }

  def addResultTuple(c: RoleMerge, componentID: String, method: String) = {
    val verticesSorted = c.clique.toIndexedSeq.sorted
    val cliqueID = verticesSorted.head
    val vertices = c.clique.map(i => verticesOrdered.vertices(i)).toIndexedSeq
    var evidenceCountTotal = 0
    var validEdges = 0
    var edgesTotal = 0
    val hasEvidence = collection.mutable.HashMap[Int, Boolean]()
    for (i <- 0 until vertices.size) {
      for (j <- (i + 1) until vertices.size) {
        edgesTotal += 1
        val l1 = vertices(i).factLineage.toFactLineage
        val l2 = vertices(j).factLineage.toFactLineage
        val evidenceInThisEdge = getEvidenceInTestPhase(l1, l2, trainTimeEnd)
        evidenceCountTotal += evidenceInThisEdge
        if (evidenceInThisEdge > 0) {
          hasEvidence.put(i, true)
          hasEvidence.put(j, true)
        }
        if (l1.tryMergeWithConsistent(l2).isDefined)
          validEdges += 1
      }
    }
    val remainsValidPercentage = validEdges / edgesTotal.toDouble
    val avgEvidencePerEdge = evidenceCountTotal / edgesTotal.toDouble
    val verticesWithAtLeastOneEdgeWithEvidence = hasEvidence.values.filter(identity).size
    val fractionOfVerticesWithEvidence = verticesWithAtLeastOneEdgeWithEvidence / vertices.size.toDouble
    pr.println(s"$componentID,$method,$cliqueID,${c.clique.size},$remainsValidPercentage,$avgEvidencePerEdge,$fractionOfVerticesWithEvidence,${c.cliqueScore},$alpha")
  }

  def addResultTuples(cliquesGreedy: collection.Seq[RoleMerge], componentID: String, method: String) = {
    cliquesGreedy.foreach(c => addResultTuple(c, componentID, method))
  }


}
