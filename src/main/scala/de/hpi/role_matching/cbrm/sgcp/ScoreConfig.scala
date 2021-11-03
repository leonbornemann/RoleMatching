package de.hpi.role_matching.cbrm.sgcp

import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}
import SparseGraphCliquePartitioningMain.args
import de.hpi.role_matching.cbrm.evidence_based_weighting.EventCounts
import de.hpi.socrata.JsonReadable

case class ScoreConfig(alpha:Float,
                       strongPositiveWeight:Float,
                       weakPositiveWeight:Float,
                       neutralWeight:Float=0.0f,
                       weakNegativeWeight:Float,
                       strongNegativeWeight:Float,
                       eventCountInLineage:Int) extends JsonWritable[ScoreConfig]{

  assert(weakNegativeWeight<0)
  assert(strongNegativeWeight<0)
  assert(strongPositiveWeight>0)
  assert(weakPositiveWeight>0)

  def computeScore(eventCounts: EventCounts) = {
    val res = alpha +
      strongPositiveWeight*eventCounts.strongPositive / eventCountInLineage +
      weakPositiveWeight*eventCounts.weakPositive / eventCountInLineage +
      neutralWeight*eventCounts.neutral / eventCountInLineage +
      weakNegativeWeight*eventCounts.weakNegative / eventCountInLineage +
      strongNegativeWeight*eventCounts.strongNegative / eventCountInLineage
    if(res >10 || res < -10) {
      println()
    }
    res
  }

}

object ScoreConfig extends JsonReadable[ScoreConfig]{

}
