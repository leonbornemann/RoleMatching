package de.hpi.dataset_versioning.db_synthesis.optimization

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{IDBasedTupleReference, TupleReference}
import de.hpi.dataset_versioning.db_synthesis.graph.field_lineage.{FieldLineageGraphEdge, FieldLineageMergeabilityGraph}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.AbstractTemporalField
import scalax.collection.GraphBase
import scalax.collection.edge.WLkUnDiEdge

import java.io.{File, PrintWriter}
import java.time.temporal.TemporalField
import scala.collection.mutable
import scala.io.Source

class MaxCliqueBasedOptimizer(subdomain: String, connectedComponentListFile: File) extends StrictLogging {

  val inputTables = Source.fromFile(connectedComponentListFile)
    .getLines()
    .toIndexedSeq
    .map(stringID => {
      val id = DecomposedTemporalTableIdentifier.fromCompositeID(stringID)
      val table = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(id)
      (id,table)
    }).toMap

  val fieldLineageMergeabilityGraph = FieldLineageMergeabilityGraph.loadSubGraph(inputTables.keySet,subdomain)
  val inputGraph = fieldLineageMergeabilityGraph.transformToOptimizationGraph(inputTables)

  def runGreedy(vertexSet:Set[TupleReference[Any]], cliquesWithScore: Map[Set[TupleReference[Any]], Double]) = {
    val coveredVertices = mutable.HashSet[TupleReference[Any]]()
    val chosenMerges = mutable.HashSet[TupleMerge]()
    val sortedCandidatesIterator = cliquesWithScore
      .toIndexedSeq
      .sortBy(-_._2)
      .iterator
    var done = false
    while(sortedCandidatesIterator.hasNext && coveredVertices.size!=vertexSet.size && !done){
      val (curClique,score) = sortedCandidatesIterator.next()
      if(score<=0){
        done = true
      } else if(curClique.intersect(coveredVertices).isEmpty){
        coveredVertices ++= curClique
        chosenMerges += TupleMerge(curClique.map(_.toIDBasedTupleReference),score)
      }
    }
    assert(coveredVertices==vertexSet)
    chosenMerges
  }

  def run() = {
    logger.debug(s"Starting Clique Partitioning Optimization for $connectedComponentListFile")
    val traverser = inputGraph.componentTraverser()
    val pr = new PrintWriter(TupleMerge.getStandardJsonObjectPerLineFile(connectedComponentListFile.getName))
    traverser.foreach(e => {
      val vertices = e.nodes.map(_.value).toSet
      val allCliques = new BronKerboshAlgorithm(e.asInstanceOf[GraphBase[TupleReference[Any], WLkUnDiEdge]])
        .run()
      val cliqueToScore = allCliques
        .map(clique => (clique,AbstractTemporalField.ENTROPY_REDUCTION_SET(clique)))
        .toMap
      val chosenMerges = runGreedy(vertices,cliqueToScore)
      chosenMerges.foreach(tm => tm.appendToWriter(pr,false,true))
      pr.flush()
      logger.debug(s"Finished connected component with size ${vertices.size}")
    })
  }

}
