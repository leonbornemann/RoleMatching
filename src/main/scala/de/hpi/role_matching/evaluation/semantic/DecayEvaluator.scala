package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.RoleLineageWithID

import java.io.PrintWriter
import java.time.LocalDate

class DecayEvaluator(resultPRDecay: PrintWriter) {

  val decayValues = (0 to 50).map(i => 1.0 - i/100.0)
  val compatibilityValues = (0 to 50).map(i => 1.0 - i/100.0)


  def addRecords(dataset: String, groundTruthExamples: Iterable[(SimpleCompatbilityGraphEdge, SimpleCompatbilityGraphEdge, Boolean)], trainTimeEnd: LocalDate) = {
    groundTruthExamples
      .foreach{case (edgeDecay,edgeNoDecay,label) =>
        val rl1 = edgeNoDecay.v1
        val rl2 = edgeNoDecay.v2
        decayValues.foreach(beta => {
          val rl1WithDecay = if(beta<1.0) rl1.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd) else rl1.roleLineage.toRoleLineage
          val rl2WithDecay = if(beta<1.0) rl2.roleLineage.toRoleLineage.applyDecay(beta,trainTimeEnd) else rl2.roleLineage.toRoleLineage
          val compatibility = rl1WithDecay.getCompatibilityTimePercentage(rl2WithDecay,trainTimeEnd)
          compatibilityValues.foreach(gamma => {
            val isInCBRB = compatibility>=gamma
            resultPRDecay.println(s"$dataset,${rl1.csvSafeID},${rl2.csvSafeID},$beta,$gamma,$isInCBRB,$label")
          })
        })
      }
  }

  resultPRDecay.println("dataset,id1,id2,betaThreshold,gammaThreshold,isInCBRB,isSemanticRoleMatch")

}
