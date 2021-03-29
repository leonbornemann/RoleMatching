package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.graph.fact.FactMergeabilityGraph
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.factLookup.FactLookupTable
import de.hpi.tfm.data.tfmp_input.table.nonSketch.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.tfm.evaluation.FieldLineageMergeEvaluationMain.tables
import de.hpi.tfm.io.IOService

object OldFactLineageStatistics extends App {
  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val statisicsgatherer = new OldFactLineageStatisticGatherer(subdomain)


}
