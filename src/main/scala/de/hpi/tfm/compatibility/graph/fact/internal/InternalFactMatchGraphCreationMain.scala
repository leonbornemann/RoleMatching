package de.hpi.tfm.compatibility.graph.fact.internal

import de.hpi.tfm.compatibility.graph.fact.FactMergeabilityGraph
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

/** *
 * Creates edges within an association (there should be none)
 */
object InternalFactMatchGraphCreationMain extends App {
  IOService.socrataDir = args(0)
  val compositeID = args(1)
  val minEvidence = args(2).toInt
  val id = AssociationIdentifier.fromCompositeID(compositeID)
  assert(id.associationID.isDefined)
  val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
  val hasChanges = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countChanges(table)._1 > 0
  if (hasChanges) {
    val tuples = table.tupleReferences
    val graph = new InternalFactMatchGraphCreator[Any](tuples)
    val graphEdges = graph.toFieldLineageMergeabilityGraph(true)
      .edges
      .filter(_.evidence>=minEvidence)
    FactMergeabilityGraph(graphEdges).writeToStandardFile()
  }
}
