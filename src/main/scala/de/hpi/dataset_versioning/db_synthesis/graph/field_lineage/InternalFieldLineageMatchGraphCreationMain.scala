package de.hpi.dataset_versioning.db_synthesis.graph.field_lineage

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.field_graph.FieldLineageMatchGraph
import de.hpi.dataset_versioning.io.IOService

/** *
 * Creates edges within an association (there should be none)
 */
object InternalFieldLineageMatchGraphCreationMain extends App {
  IOService.socrataDir = args(0)
  val compositeID = args(1)
  val id = DecomposedTemporalTableIdentifier.fromCompositeID(compositeID)
  assert(id.associationID.isDefined)
  val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
  val hasChanges = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(table)._1 > 0
  if (hasChanges) {
    val tuples = table.tupleReferences
    val graph = new FieldLineageMatchGraph[Any](tuples)
    graph.toFieldLineageMergeabilityGraph(true).writeToStandardFile()
  }
}
