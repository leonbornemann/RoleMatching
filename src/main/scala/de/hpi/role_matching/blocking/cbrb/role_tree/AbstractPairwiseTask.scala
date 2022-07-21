package de.hpi.role_matching.blocking.cbrb.role_tree

abstract class AbstractPairwiseTask() {

  def exceedsThreshold(BATCH_SIZE: Int): Boolean = totalMatchChecks >= BATCH_SIZE

  var totalMatchChecks = 0
}
