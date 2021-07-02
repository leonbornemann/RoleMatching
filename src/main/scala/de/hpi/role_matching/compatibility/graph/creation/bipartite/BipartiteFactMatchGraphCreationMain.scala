package de.hpi.role_matching.compatibility.graph.creation.bipartite

import com.typesafe.scalalogging.StrictLogging

/** *
 * Creates edges between two associations
 */
object BipartiteFactMatchGraphCreationMain extends App with StrictLogging {
  assert(false) //TODO: correct the following to work with the parallelized variant!
  //  IOService.socrataDir = args(0)
//  val edges = AssociationGraphEdgeCandidate.fromJsonObjectPerLineFile(args(1))
//  val tables = scala.collection.mutable.HashMap[AssociationIdentifier,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation]()
//  val minEvidence = args(2).toInt
//  val timeRangeStart = LocalDate.parse(args(3))
//  val timeRangeEnd = LocalDate.parse(args(4))
//  val graphConfig = GraphConfig(minEvidence,timeRangeStart,timeRangeEnd)
//
//  def getOrLoad(id: AssociationIdentifier) = {
//    if(timeRangeStart==IOService.STANDARD_TIME_FRAME_START && timeRangeEnd == IOService.STANDARD_TIME_FRAME_END){
//      tables.getOrElseUpdate(id,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id))
//    } else {
//      tables.getOrElseUpdate(id,SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFomFullTimeRangeFile(id)
//        .projectToTimeRange(timeRangeStart,timeRangeEnd))
//    }
//  }
//  val nonInformativeValues:Set[Any] = Set("",null)
//  for (edge <- edges) {
//    logger.debug(s"Discovering mergeability for $edge")
//    val tableLeft = getOrLoad(edge.firstMatchPartner)
//    val tableRight = getOrLoad(edge.secondMatchPartner)
//    val matchGraphEdges = new BipartiteFactMatchCreator(tableLeft.tupleReferences,
//      tableRight.tupleReferences,
//      graphConfig,true,
//      None,
//      nonInformativeValues)
//      .toFieldLineageMergeabilityGraph(true)
//      .edges
//      .filter(_.evidence>=minEvidence)
//    val matchGraph = FactMergeabilityGraph(matchGraphEdges,graphConfig)
//    logger.debug(s"Found ${matchGraph.edges.size} edges ")
//    if (matchGraph.edges.size > 0) {
//      matchGraph.writeToStandardFile()
//      val tg = matchGraph.transformToAssociationGraph
//      assert(tg.edges.size <= 1)
//      if (tg.edges.size > 0) {
//        val filename = tg.edges.head.v1.compositeID + ";" + tg.edges.head.v2.compositeID + ".json"
//        val subdomain = tg.edges.head.v1.subdomain
//        tg.writeToSingleEdgeFile(filename, subdomain,graphConfig)
//      }
//    }
//  }
}
