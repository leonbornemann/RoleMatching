package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

object TransitionHistogramMode extends Enumeration {
  type TransitionHistogramMode = Value
  val NORMAL,COUNT_CONSECUTIVE_NON_CHANGE_ONLY_ONCE,IGNORE_NON_CHANGE = Value
}
