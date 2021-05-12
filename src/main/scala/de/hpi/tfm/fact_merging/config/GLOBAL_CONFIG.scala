package de.hpi.tfm.fact_merging.config

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.AbstractTemporalField
import de.hpi.tfm.fact_merging.metrics.{EdgeScore, MultipleEventWeightScore}
import de.hpi.tfm.fact_merging.optimization.{ConnectedComponentMergeOptimizer, GreedyMaxCliqueBasedOptimizer}

import java.io.File
import java.lang
import java.lang.AssertionError
import java.time.LocalDate

object GLOBAL_CONFIG {

  def getOptimizer(optimizationMethodName: String, subdomain: String, connectedComponentFile: File, graphConfig: GraphConfig):ConnectedComponentMergeOptimizer = {
    optimizationMethodName match {
      case GreedyMaxCliqueBasedOptimizer.methodName => new GreedyMaxCliqueBasedOptimizer(subdomain,connectedComponentFile,graphConfig)
      case _ => throw new AssertionError(s"$optimizationMethodName not known")
    }
  }

  var granularityInDays = Int.MaxValue
  var trainTimeEnd = LocalDate.MIN

  def nameToFunction:Map[String,EdgeScore] = {
    if(granularityInDays == Int.MaxValue || trainTimeEnd==LocalDate.MIN)
      throw new AssertionError("Config not complete!")
    Map((MultipleEventWeightScore.name,new MultipleEventWeightScore(granularityInDays,trainTimeEnd)))
  }

  var OPTIMIZATION_TARGET_FUNCTION_NAME:String = ""

  def OPTIMIZATION_TARGET_FUNCTION[A](tr1: TupleReference[A]) = nameToFunction(OPTIMIZATION_TARGET_FUNCTION_NAME).compute(tr1)
  //def OPTIMIZATION_TARGET_FUNCTION[A](tr1: TupleReference[A], tr2: TupleReference[A]) = AbstractTemporalField.MUTUAL_INFORMATION(tr1,tr2)
  def OPTIMIZATION_TARGET_FUNCTION[A](tr1: TupleReference[A], tr2: TupleReference[A]) = nameToFunction(OPTIMIZATION_TARGET_FUNCTION_NAME).compute(tr1,tr2)

  var ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS = false

  //val CHANGE_COUNT_METHOD = new DatasetInsertIgnoreFieldChangeCounter()
  val CHANGE_COUNT_METHOD = new UpdateChangeCounter()
}
