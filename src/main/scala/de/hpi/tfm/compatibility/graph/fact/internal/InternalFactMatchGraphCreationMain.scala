package de.hpi.tfm.compatibility.graph.fact.internal

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.FactMergeabilityGraph
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG
import de.hpi.tfm.io.IOService

import java.time.LocalDate

/** *
 * Creates edges within an association (there should be none)
 */
object InternalFactMatchGraphCreationMain extends App {
  assert(false) //TODO: correct the following to work with the parallelized variant!

//  IOService.socrataDir = args(0)
//  val compositeID = args(1)
//  val minEvidence = args(2).toInt
//  val timeRangeStart = LocalDate.parse(args(3))
//  val timeRangeEnd = LocalDate.parse(args(4))
//  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
//  val id = AssociationIdentifier.fromCompositeID(compositeID)
//  assert(id.associationID.isDefined)
//  val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
//  val hasChanges = GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countChanges(table)._1 > 0
//  if (hasChanges) {
//    val tuples = table.tupleReferences
//    val graph = new InternalFactMatchGraphCreator[Any](tuples,graphConfig)
//    val graphEdges = graph.toFieldLineageMergeabilityGraph(true)
//      .edges
//      .filter(_.evidence>=minEvidence)
//    FactMergeabilityGraph(graphEdges,graphConfig).writeToStandardFile()
//  }
}
