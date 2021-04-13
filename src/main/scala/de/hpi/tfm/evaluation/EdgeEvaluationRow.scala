package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.graph.fact.IDBasedTupleReference

case class EdgeEvaluationRow(tupleReference1:IDBasedTupleReference,
                             tupleReference2:IDBasedTupleReference,
                             remainsValid:Boolean,
                             hasChangeAfterTrainPeriod:Boolean,
                             numDaysUntilRealChangeAfterTrainPeriod:Int,
                             numEqualTransitions:Int,
                             numUnEqualTransitions:Int,
                             numEqualChangeTransitions:Int,
                             mutualInformationScore:Double,
                             newScore:Double){

  def toCSVRow = Seq(tupleReference1,
    tupleReference2,
    remainsValid,
    hasChangeAfterTrainPeriod,
    numDaysUntilRealChangeAfterTrainPeriod,
    numEqualTransitions,
    numUnEqualTransitions,
    numEqualChangeTransitions,
    mutualInformationScore,
    newScore).mkString(",")

}



object EdgeEvaluationRow {
  def schema = Seq("tupleReference1",
    "tupleReference2",
    "remainsValid",
    "hasChangeAfterTrainPeriod",
    "numDaysUntilRealChangeAfterTrainPeriod",
    "numEqualTransitions",
    "numUnEqualTransitions",
    "numEqualChangeTransitions",
    "mutualInformationScore",
    "newScore").mkString(",")

}
