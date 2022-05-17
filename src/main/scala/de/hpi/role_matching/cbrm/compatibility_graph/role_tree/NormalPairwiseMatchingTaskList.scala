package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import scala.collection.mutable.ArrayBuffer

case class NormalPairwiseMatchingTaskList(taskList:ArrayBuffer[NormalPairwiseMatchingTask] = ArrayBuffer[NormalPairwiseMatchingTask]()) extends AbstractPairwiseMatchingTask{

  def appendTask(newTask: NormalPairwiseMatchingTask) = {
    totalMatchChecks += newTask.totalMatchChecks
    taskList.append(newTask)
  }

}
