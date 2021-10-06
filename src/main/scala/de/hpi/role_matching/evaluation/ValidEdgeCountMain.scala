package de.hpi.role_matching.evaluation

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.graph.representation.slim.SlimGraphWithoutWeight
import de.hpi.role_matching.evaluation.clique.CliqueBasedEvaluationMain.args

import java.io.File
import java.time.LocalDate

object ValidEdgeCountMain extends App {
  val graphFile = args(0)
  val source = args(1)
  GLOBAL_CONFIG.setDatesForDataSource(source)
  val trainTimeEnd = LocalDate.parse(args(2))
  val graph = SlimGraphWithoutWeight.fromJsonFile(graphFile)
  val validEdgeCounter = new ValidEdgeCounter(graph,trainTimeEnd)
  validEdgeCounter.printCount()

}
