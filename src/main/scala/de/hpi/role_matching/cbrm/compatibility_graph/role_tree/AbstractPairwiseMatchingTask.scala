package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

abstract class AbstractPairwiseMatchingTask() {

  def exceedsThreshold(BATCH_SIZE: Int): Boolean = totalMatchChecks >= BATCH_SIZE

  var totalMatchChecks = 0
}
