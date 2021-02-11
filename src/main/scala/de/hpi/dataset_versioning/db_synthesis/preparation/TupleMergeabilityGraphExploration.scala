package de.hpi.dataset_versioning.db_synthesis.preparation

import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object TupleMergeabilityGraphExploration extends App {

  IOService.socrataDir = args(0)
  val subdomain = args(1)
  val fileCountLimit = if(args.length==3) args(2).toInt else Integer.MAX_VALUE
  val associationMergeabilityGraph = FieldLineageMergeabilityGraph.readFieldLineageMergeabilityGraphAndAggregateToTableGraph(subdomain,fileCountLimit)
  associationMergeabilityGraph.writeToStandardFile(subdomain)


}
