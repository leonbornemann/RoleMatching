package de.hpi.role_matching.evaluation.exploration

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.compatibility.graph.representation.vertex.{IdentifiedFactLineage, VerticesOrdered}
import de.hpi.role_matching.clique_partitioning.RoleMerge
import de.hpi.wikipedia.data.transformed.WikipediaInfoboxPropertyID

object CliqueMatchExplorationMain extends App with StrictLogging {
  val matchFile = args(0)
  val vertexOrderedFile = args(1)
  val matches = RoleMerge.fromJsonObjectPerLineFile(matchFile)
  val vertices = VerticesOrdered.fromJsonFile(vertexOrderedFile)
  val filtered = matches.filter(m => m.clique.size > 3 && m.clique.size < 10)
  val cliques = filtered.map(itm => (itm.clique.map(i => (i, WikipediaInfoboxPropertyID.from(vertices.vertices(i).id))), itm.cliqueScore))
  cliques.foreach { case (c, score) => {
    val verticesThisClique = c.map(t => vertices.vertices(t._1)).toSeq.sortBy(_.id)
    var remainsValid = true
    for (i <- (0 until verticesThisClique.size)) {
      for (j <- (i + 1 until verticesThisClique.size)) {
        remainsValid = remainsValid && verticesThisClique(i).factLineage.toFactLineage.tryMergeWithConsistent(verticesThisClique(j).factLineage.toFactLineage).isDefined
      }
    }
    logger.debug("--------------------------------------------------------------------------------------------------------------------------")
    logger.debug(s"Found clique ${c} with score $score, remains valid: $remainsValid")
    logger.debug("Lineages:")
    IdentifiedFactLineage.printTabularEventLineageString(verticesThisClique)
    logger.debug(s"Wikilinks:")
    c.foreach { case (_, id) => logger.debug(s"${id.toWikipediaURLInfo}") }
    logger.debug("--------------------------------------------------------------------------------------------------------------------------")
  }
  }
}
