package de.hpi.tfm.fact_merging.optimization

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.JsonWritable
import de.hpi.tfm.evaluation.data.SLimGraph

import java.time.LocalDate

//if Seq[EventCountsWithoutWeights].size in an edge is smaller than trainTimeEnds.size the later points in time do not exist
case class SlimGraphSet(verticesOrdered: IndexedSeq[String],
                        trainTimeEnds:Seq[LocalDate],
                        adjacencyList: collection.Map[Int, collection.Map[Int, Seq[EventCountsWithoutWeights]]]) extends JsonWritable[SLimGraph] with StrictLogging{

}
