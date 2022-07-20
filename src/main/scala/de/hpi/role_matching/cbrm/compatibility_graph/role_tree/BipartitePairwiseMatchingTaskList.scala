package de.hpi.role_matching.cbrm.compatibility_graph.role_tree
import scala.collection.mutable.ArrayBuffer

case class BipartitePairwiseMatchingTaskList(taskList:ArrayBuffer[BipartitePairwiseMatchingTask] = ArrayBuffer())  extends AbstractPairwiseMatchingTask{

  def append(newTask: BipartitePairwiseMatchingTask) = {
    totalMatchChecks += newTask.totalMatchChecks
    taskList.append(newTask)
  }

}
