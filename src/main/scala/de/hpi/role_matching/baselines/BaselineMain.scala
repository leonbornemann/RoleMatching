package de.hpi.role_matching.baselines

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.sgcp.RoleMerge
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}
import de.hpi.role_matching.evaluation.matching.RoleMatchinEvaluator
import de.hpi.role_matching.evaluation.matching.RoleMatchingEvaluationMain.resultDir

import java.io.PrintWriter
import java.time.LocalDate
import scala.io.Source

/***
 * Can run either Value Set Baseline or Value Sequence Baseline
 */
object BaselineMain extends App with StrictLogging{
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val vertexLookupMap = Roleset.fromJsonFile(args(0))
  val dataSource = args(1)
  GLOBAL_CONFIG.setDatesForDataSource(dataSource)
  val trainTimeEnd = LocalDate.parse(args(2))
  val resultDir = args(3)
  val methodIsValueSet = args(4) == "valueSet"
  if(!methodIsValueSet)
    assert(args(4) == "valueSequence")
  val maxRecallEdgeSetFile = args(5)
  val prCliques = new PrintWriter(resultDir + "/cliques.csv")
  val prCliquesTruePositivesToReview = new PrintWriter(resultDir + "/cliques_To_Review_True_positives.csv")
  val prCliquesRestToReview = new PrintWriter(resultDir + "/cliques_To_Review_Rest.csv")
  val tableStringPr = new PrintWriter(resultDir + "/tableStrings.txt")
  val prEdges = new PrintWriter(resultDir + "/edges.csv")
  val grouped = vertexLookupMap.positionToRoleLineage.groupMap(ifl => {
    if(methodIsValueSet)
      ifl._2.factLineage.toFactLineage.nonWildcardValueSetBefore(trainTimeEnd)
    else
      ifl._2.factLineage.toFactLineage.nonWildcardValueSequenceBefore(trainTimeEnd)
  })(_._1)
  val edgesInMaxRecall = Source.fromFile(maxRecallEdgeSetFile)
    .getLines()
    .toIndexedSeq
    .tail
    .map(s => s.split(",")(1))
    .toSet
  val analyzer = new RoleMatchinEvaluator(prCliques,prCliquesTruePositivesToReview,prCliquesRestToReview,tableStringPr,prEdges,vertexLookupMap,trainTimeEnd,None,None,Some(edgesInMaxRecall))
  analyzer.serializeSchema()
  var groupsDone = 0
  val method = if(methodIsValueSet) "valueSetBaseline" else "valueSequenceBaseline"
  grouped.values
    .withFilter(_.size>1)
    .foreach(matched => {
    val rm = RoleMerge(matched.toSet,Double.MinValue)
    if(matched.size*matched.size>100000){
      logger.debug(s"Processing large group with ${matched.size} vertices")
    }
      analyzer.addResultTuple(rm,"NA",method)
    if(matched.size*matched.size>100000){
      logger.debug(s"Done with large group with ${matched.size} vertices")
    }
    groupsDone+=1
    if(groupsDone%1000 == 0){
      logger.debug(s"Finished $groupsDone (${100*groupsDone / grouped.size.toDouble}%)")
    }
  })
  analyzer.printResults()
  prEdges.close()
  prCliques.close()

}
