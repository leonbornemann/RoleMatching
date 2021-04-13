package de.hpi.tfm.evaluation

import de.hpi.tfm.compatibility.graph.fact.IDBasedTupleReference

//mergedLineages (somehow encode),cliqueSize,remainsValid,remainsInteresting,totalScore,scoreName,optimizationMethodName
case class CliqueEvaluationRow(optimizationMethodName:String,
                               edgeScoreName:String,
                               componentFile:String,
                               mergedLineages:IndexedSeq[IDBasedTupleReference],
                               cliqueSize:Int,
                               numEdgesInClique:Int, //for convenience
                               remainsValid:Boolean,
                               numValidEdges:Int,
                               numVerticesWithChangeAfterTrainPeriod:Int,
                               totalScore:Double,
                               scoreAggregateMethodName:String){
  def toCSVRowString():String = Seq(
    optimizationMethodName,
    edgeScoreName,
    componentFile,
    mergedLineages.mkString(";"),
    cliqueSize,
    numEdgesInClique, //for convenience
    remainsValid,
    numValidEdges,
    numVerticesWithChangeAfterTrainPeriod,
    totalScore,
    scoreAggregateMethodName
  ).mkString(",")

}
object CliqueEvaluationRow{
  def schema = Seq("optimizationMethodName",
    "edgeScoreName",
    "mergedLineages",
    "cliqueSize",
    "numEdgesInClique",
    "remainsValid",
    "numValidEdges",
    "numVerticesWithChangeAfterTrainPeriod",
    "totalScore",
    "scoreAggregateMethodName")
}