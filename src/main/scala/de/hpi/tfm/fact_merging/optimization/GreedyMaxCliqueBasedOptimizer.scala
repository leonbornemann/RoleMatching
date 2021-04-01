package de.hpi.tfm.fact_merging.optimization

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.AbstractTemporalField
import scalax.collection.Graph
import scalax.collection.edge.WLkUnDiEdge

import java.io.{File, PrintWriter}
import scala.collection.mutable

class GreedyMaxCliqueBasedOptimizer(subdomain: String, connectedComponentListFile: File,graphConfig:GraphConfig) extends ConnectedComponentMergeOptimizer(subdomain,connectedComponentListFile,graphConfig) with StrictLogging {

  val methodName = "GreedyMaxCliqueBasedOptimizer"

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
    val pr = new PrintWriter(TupleMerge.getStandardJsonObjectPerLineFile(subdomain,methodName,connectedComponentListFile.getName))
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
}
