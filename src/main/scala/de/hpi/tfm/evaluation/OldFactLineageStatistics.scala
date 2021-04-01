package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.FactMergeabilityGraph
import de.hpi.tfm.compatibility.graph.fact.bipartite.DirectBipartiteFactMatchGraphCreationMain.args
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.factLookup.FactLookupTable
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.tfm.evaluation.FieldLineageMergeEvaluationMain.tables
import de.hpi.tfm.io.IOService

import java.time.LocalDate

object OldFactLineageStatistics extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val minEvidence = args(2).toInt
  val timeRangeStart = LocalDate.parse(args(3))
  val timeRangeEnd = LocalDate.parse(args(4))
  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
  val statisicsgatherer = new OldFactLineageStatisticGatherer(subdomain,graphConfig)
  statisicsgatherer.gather()
}
