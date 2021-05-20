package de.hpi.tfm.fact_merging.config

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.fact_merging.metrics.{EdgeScore, MultipleEventWeightScore}
import de.hpi.tfm.fact_merging.optimization.old.{ConnectedComponentMergeOptimizer, GreedyMaxCliqueBasedOptimizer}

import java.io.File
import java.time.LocalDate

object GLOBAL_CONFIG {

  def getOptimizer(optimizationMethodName: String, subdomain: String, connectedComponentFile: File, graphConfig: GraphConfig):ConnectedComponentMergeOptimizer = {
    optimizationMethodName match {
      case GreedyMaxCliqueBasedOptimizer.methodName => new GreedyMaxCliqueBasedOptimizer(subdomain,connectedComponentFile,graphConfig)
      case _ => throw new AssertionError(s"$optimizationMethodName not known")
    }
  }

  var nonInformativeValues:Set[Any]= Set("",null)

  var granularityInDays = Int.MaxValue
  var trainTimeEnd = LocalDate.MIN

  var OPTIMIZATION_TARGET_FUNCTION_NAME:String = ""
  var ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS = false

  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val CHANGE_COUNT_METHOD = new UpdateChangeCounter()
}
