package de.hpi.role_matching.clique_partitioning

import de.hpi.role_matching.clique_partitioning.SparseGraphCliquePartitioningMain.args
import de.hpi.role_matching.scoring.EventCountsWithoutWeights
import de.hpi.socrata.{JsonReadable, JsonWritable}

case class ScoreConfig(alpha:Float,
                       strongPositiveWeight:Float,
                       weakPositiveWeight:Float,
                       neutralWeight:Float=0.0f,
                       weakNegativeWeight:Float,
                       strongNegativeWeight:Float) extends JsonWritable[ScoreConfig]{

  assert(weakNegativeWeight<0)
  assert(strongNegativeWeight<0)
  assert(strongPositiveWeight>0)
  assert(weakPositiveWeight>0)

  def computeScore(eventCounts: EventCountsWithoutWeights) = {
    alpha +
      strongPositiveWeight*eventCounts.strongPositive +
      weakPositiveWeight*eventCounts.weakPositive +
      neutralWeight*eventCounts.neutral +
      weakNegativeWeight*eventCounts.weakNegative +
      strongNegativeWeight*eventCounts.strongNegative
  }

}

object ScoreConfig extends JsonReadable[ScoreConfig]{
  def fromCLIArguments(weighStr: String,alpahStr:String) = {
    val weights = weighStr.split(";").map(_.toFloat)
    assert(weights.size==5)
    ScoreConfig(alpahStr.toFloat,weights(0),weights(1),weights(2),weights(3),weights(4))
  }

}
