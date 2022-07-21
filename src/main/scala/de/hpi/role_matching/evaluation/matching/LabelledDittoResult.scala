package de.hpi.role_matching.evaluation.matching

case class LabelledDittoResult(dataset:String, predictedIsMatch: Boolean, trueIsMatch: Boolean, match_confidence: Double) {
  def csvLine = s"$dataset,$predictedIsMatch,$trueIsMatch,$match_confidence"
}

object LabelledDittoResult {
  def schema = "dataset, predictedIsMatch, trueIsMatch, match_confidence"
}
