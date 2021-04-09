package de.hpi.tfm.fact_merging.config

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.AbstractTemporalField
import de.hpi.tfm.fact_merging.metrics.MultipleEventWeightScore
import de.hpi.tfm.fact_merging.optimization.{ConnectedComponentMergeOptimizer, GreedyEdgeWeightOptimizer, GreedyMaxCliqueBasedOptimizer}

import java.io.File
import java.lang.AssertionError

object GLOBAL_CONFIG {

  def getOptimizer(optimizationMethodName: String, subdomain: String, connectedComponentFile: File, graphConfig: GraphConfig):ConnectedComponentMergeOptimizer = {
    optimizationMethodName match {
      case GreedyEdgeWeightOptimizer.methodName => new GreedyEdgeWeightOptimizer(subdomain,connectedComponentFile,graphConfig)
      case GreedyMaxCliqueBasedOptimizer.methodName => new GreedyMaxCliqueBasedOptimizer(subdomain,connectedComponentFile,graphConfig)
      case _ => throw new AssertionError(s"$optimizationMethodName not known")
    }
  }


  val nameToFunction = Map((MultipleEventWeightScore.name,new MultipleEventWeightScore()))

  var OPTIMIZATION_TARGET_FUNCTION_NAME:String = ""

  //def OPTIMIZATION_TARGET_FUNCTION[A](tr1: TupleReference[A], tr2: TupleReference[A]) = AbstractTemporalField.MUTUAL_INFORMATION(tr1,tr2)
  def OPTIMIZATION_TARGET_FUNCTION[A](tr1: TupleReference[A], tr2: TupleReference[A]) = nameToFunction(OPTIMIZATION_TARGET_FUNCTION_NAME).compute(tr1,tr2)

  var ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS = false

  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val CHANGE_COUNT_METHOD = new UpdateChangeCounter()
}
