package de.hpi.role_matching.blocking.cbrb.role_tree.normal

import de.hpi.role_matching.blocking.cbrb.role_tree.AbstractPairwiseTask

import scala.collection.mutable.ArrayBuffer

case class NormalPairwiseTaskList(taskList: ArrayBuffer[NormalPairwiseMatchingTask] = ArrayBuffer[NormalPairwiseMatchingTask]()) extends AbstractPairwiseTask {

  def appendTask(newTask: NormalPairwiseMatchingTask) = {
    totalMatchChecks += newTask.totalMatchChecks
    taskList.append(newTask)
  }

}
