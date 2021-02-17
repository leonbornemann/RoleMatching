package de.hpi.dataset_versioning.db_synthesis.optimization

import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{IDBasedTupleReference, TupleReference}
import de.hpi.dataset_versioning.db_synthesis.graph.field_lineage.{FieldLineageGraphEdge, FieldLineageMergeabilityGraph}
import scalax.collection.GraphBase
import scalax.collection.edge.WLkUnDiEdge

import java.io.File
import scala.io.Source

class MaxCliqueBasedOptimizer(subdomain: String, connectedComponentListFile: File) {

  val inputTables = Source.fromFile(connectedComponentListFile)
    .getLines()
    .toIndexedSeq
    .map(stringID => {
      val id = DecomposedTemporalTableIdentifier.fromCompositeID(stringID)
      val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
      (id,table)
    }).toMap

  val fieldLineageMergeabilityGraph = FieldLineageMergeabilityGraph.loadSubGraph(inputTables.keySet,subdomain)
  val graphToCliquePartition = fieldLineageMergeabilityGraph.transformToOptimizationGraph(inputTables)

  def run() = {
    val traverser = graphToCliquePartition.componentTraverser()

    traverser.foreach(e => {

      val allCliques = new BronKerboshAlgorithm(e.asInstanceOf[GraphBase[TupleReference[Any], WLkUnDiEdge]])
    })
  }

}
