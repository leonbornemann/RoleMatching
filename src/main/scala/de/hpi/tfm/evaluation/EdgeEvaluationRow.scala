package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.graph.fact.IDBasedTupleReference

case class EdgeEvaluationRow(tupleReference1:IDBasedTupleReference,
                             tupleReference2:IDBasedTupleReference,
                             remainsValid:Boolean,
                             hasChangeAfterTrainPeriod:Boolean,
                             numEqualTransitions:Int,
                             numUnEqualTransitions:Int,
                             numEqualChangeTransitions:Int,
                             mutualInformationScore:Double,
                             newScore:Double){

  def toCSVRow = Seq(tupleReference1,
    tupleReference2,
    remainsValid,
    hasChangeAfterTrainPeriod,
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
    "numEqualTransitions",
    "numUnEqualTransitions",
    "numEqualChangeTransitions",
    "mutualInformationScore",
    "newScore").mkString(",")

}
