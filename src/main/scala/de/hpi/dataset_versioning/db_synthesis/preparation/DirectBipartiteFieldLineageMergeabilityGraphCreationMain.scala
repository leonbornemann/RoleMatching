package de.hpi.dataset_versioning.db_synthesis.preparation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{AssociationGraphEdge, BipartiteFieldLineageMatchGraph}
import de.hpi.dataset_versioning.io.IOService


/***
 * Creates edges between two associations
 */
object DirectBipartiteFieldLineageMergeabilityGraphCreationMain extends App with StrictLogging {
  IOService.socrataDir = args(0)
  val subdomain = "org.cityofchicago"
  val id1 = DecomposedTemporalTableIdentifier.fromShortString(subdomain,"pubx-yq2d.0_17")
  val id2 = DecomposedTemporalTableIdentifier.fromShortString(subdomain,"pubx-yq2d.0_2")
  val matchGraphRead = FieldLineageMergeabilityGraph.readFromStandardFile(Set(id1,id2))
  val isNotOK = matchGraphRead.edges.flatMap(_.evidenceSet.get).exists(e => e._1.prev==e._1.after)
  val tableLeft = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id1)
  val tableRight = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id2)
  val leftTableHasChanges = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(tableLeft)._1 > 0
  val rightTableHasChanges = GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD.countChanges(tableRight)._1 > 0
  if(leftTableHasChanges && rightTableHasChanges){
    val matchGraph = new BipartiteFieldLineageMatchGraph(tableLeft.tupleReferences,tableRight.tupleReferences)
      .toFieldLineageMergeabilityGraph(true)
    logger.debug(s"Found ${matchGraph.edges.size} edges of which ${matchGraph.edges.filter(_.evidence>0).size} have more than 0 evidence ")
    if(matchGraph.edges.size>0)
      matchGraph.writeToStandardFile()
  }
  //pubx-yq2d.0_17,pubx-yq2d.0_2
}
