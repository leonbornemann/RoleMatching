package de.hpi.tfm.data.wikipedia.infobox.fact_merging

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxPropertyID
import de.hpi.tfm.evaluation.data.{IdentifiedTupleMerge, SLimGraph}

object CliqueMatchExplorationMain extends App with StrictLogging{
  val graphFile = args(0)
  val matchFile = args(1)
  val slimGraph = SLimGraph.fromJsonFile(graphFile)
  val matches = IdentifiedTupleMerge.fromJsonObjectPerLineFile(matchFile)
  val filtered = matches.filter(m => m.clique.size>3 && m.clique.size<10)
  val cliques = filtered.map(itm => (itm.clique.map(i => WikipediaInfoboxPropertyID.from(slimGraph.verticesOrdered(i))),itm.cliqueScore))
  cliques.foreach{case (c,score) => {
    logger.debug("--------------------------------------------------------------------------------------------------------------------------")
    logger.debug(s"Found clique ${c} with score $score")
    logger.debug(s"Wikilinks:")
    c.foreach(id => logger.debug(s"${id.toWikipediaURLInfo}"))
    logger.debug("--------------------------------------------------------------------------------------------------------------------------")
  }}
}
