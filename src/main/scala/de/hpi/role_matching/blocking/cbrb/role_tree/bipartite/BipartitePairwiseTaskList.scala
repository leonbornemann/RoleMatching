package de.hpi.role_matching.blocking.cbrb.role_tree.bipartite

import de.hpi.role_matching.blocking.cbrb.role_tree.AbstractPairwiseTask

import scala.collection.mutable.ArrayBuffer

case class BipartitePairwiseTaskList(taskList: ArrayBuffer[BipartitePairwiseMatchingTask] = ArrayBuffer()) extends AbstractPairwiseTask {

  def append(newTask: BipartitePairwiseMatchingTask) = {
    totalMatchChecks += newTask.totalMatchChecks
    taskList.append(newTask)
  }

}
