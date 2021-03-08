package de.hpi.dataset_versioning.db_synthesis.optimization

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{IDBasedTupleReference, TupleReference}
import de.hpi.dataset_versioning.db_synthesis.graph.field_lineage.{FieldLineageGraphEdge, FieldLineageMergeabilityGraph}
import de.hpi.dataset_versioning.db_synthesis.sketches.field.AbstractTemporalField
import scalax.collection.{Graph, GraphBase, GraphTraversalImpl}
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
    if(coveredVertices!=vertexSet){
      logger.debug("Did not cover all vertices in connected component via a clique!")
      val missing = vertexSet.diff(coveredVertices)
      logger.debug(s"Missing in cover: $missing")
    }
    chosenMerges
  }

  def run() = {
    logger.debug(s"Starting Clique Partitioning Optimization for $connectedComponentListFile")
    val traverser = inputGraph.componentTraverser()
    val pr = new PrintWriter(TupleMerge.getStandardJsonObjectPerLineFile(connectedComponentListFile.getName))
    var totalScore = 0.0
    var tupleReductionCount = 0
    val cliqueSizeHistogram = mutable.HashMap[Int,Int]()
    traverser.foreach(e => {
      val subGraph: Graph[TupleReference[Any], WLkUnDiEdge] = componentToGraph(e)
      val vertices = e.nodes.map(_.value).toSet
      val allCliques = new BronKerboshAlgorithm(subGraph)
        .run()
      val cliqueToScore = allCliques
        .map(clique => (clique,AbstractTemporalField.ENTROPY_REDUCTION_SET(clique)))
        .toMap
      val chosenMerges = runGreedy(vertices,cliqueToScore)
      chosenMerges.foreach(tm => tm.appendToWriter(pr,false,true))
      pr.flush()
      logger.debug(s"Finished connected component with size ${vertices.size}")
      //Update some stats:
      totalScore += chosenMerges.toIndexedSeq.map(_.score).sum
      tupleReductionCount += chosenMerges.map(tm => tm.clique.size-1).sum
      cliqueToScore.foreach(c => {
        val old = cliqueSizeHistogram.getOrElse(c._1.size,0)
        cliqueSizeHistogram.put(c._1.size,old+1)
      })
    })
    val initialTupleCount = inputTables.values.map(_.nrows).sum
    //val allEntropies = inputTables.values.toIndexedSeq.flatMap(_.rows.map(_.valueLineage.getEntropy()))
    val totalEntropyBefore = inputTables.values.toIndexedSeq.flatMap(_.rows.map(_.valueLineage.getEntropy())).sum
    val totalEntropyAfter = totalEntropyBefore - totalScore
    val newTupleCount = initialTupleCount - tupleReductionCount
    logger.debug(s"Reduced number of tuples from $initialTupleCount to $newTupleCount")
    logger.debug(s"Reduced Entropy from $totalEntropyBefore to $totalEntropyAfter - IMPORTANT: THIS IS ONLY CORRECT IF WE ARE OPTIMIZING FOR ENTROPY SUM")
    logger.debug("Clique Sizes:")
    println("Clique Size,#Cliques")
    cliqueSizeHistogram
      .toIndexedSeq
      .sortBy(_._1)
      .foreach(t => println(s"${t._1},${t._2}"))
  }

  private def componentToGraph(e: inputGraph.Component) = {
    val vertices = e.nodes.map(_.value).toSet
    val edges = e.edges.map(e => {
      val nodes = e.toIndexedSeq.map(_.value)
      assert(nodes.size == 2)
      WLkUnDiEdge(nodes(0), nodes(1))(e.weight, e.label)
    })
    val subGraph = Graph.from(vertices, edges)
    subGraph
  }
}
