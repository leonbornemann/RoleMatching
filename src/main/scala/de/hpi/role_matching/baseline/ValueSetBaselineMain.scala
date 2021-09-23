package de.hpi.role_matching.baseline

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.clique_partitioning.RoleMerge
import de.hpi.role_matching.clique_partitioning.SparseGraphCliquePartitioningMain.{args, maxRecallSetting}
import de.hpi.role_matching.compatibility.graph.representation.slim.VertexLookupMap
import de.hpi.role_matching.compatibility.graph.representation.vertex.IdentifiedFactLineage
import de.hpi.role_matching.evaluation.clique.CliqueAnalyser
import de.hpi.role_matching.evaluation.clique.CliqueBasedEvaluationMain.resultDir

import java.io.PrintWriter
import java.time.LocalDate

object ValueSetBaselineMain extends App with StrictLogging{
  logger.debug(s"Called with ${args.toIndexedSeq}")
  val vertexLookupMap = VertexLookupMap.fromJsonFile(args(0))
  val dataSource = args(1)
  GLOBAL_CONFIG.setDatesForDataSource(dataSource)
  val trainTimeEnd = LocalDate.parse(args(2))
  val resultDir = args(3)
  val prCliques = new PrintWriter(resultDir + "/cliques.csv")
  val prEdges = new PrintWriter(resultDir + "/edges.csv")
  val grouped = vertexLookupMap.posToLineage.groupMap(ifl => {
    ifl._2.factLineage.toFactLineage.nonWildcardValueSetBefore(trainTimeEnd)
  })(_._1)
  val analyzer = new CliqueAnalyser(prCliques,prEdges,vertexLookupMap,trainTimeEnd,None,None)
  analyzer.serializeSchema()
  var groupsDone = 0
  grouped.values.foreach(matched => {
    val rm = RoleMerge(matched.toSet,Double.MinValue)
    if(matched.size*matched.size>100000){
      logger.debug(s"Processing large group with ${matched.size} vertices")
    }
    analyzer.addResultTuple(rm,"NA","valueSetBaseline")
    if(matched.size*matched.size>100000){
      logger.debug(s"Done with large group with ${matched.size} vertices")
    }
    groupsDone+=1
    if(groupsDone%1000 == 0){
      logger.debug(s"Finished $groupsDone (${100*groupsDone / grouped.size.toDouble}%)")
    }
  })
  prEdges.close()
  prCliques.close()

}
