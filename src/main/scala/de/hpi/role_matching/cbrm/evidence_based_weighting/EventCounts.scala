package de.hpi.role_matching.cbrm.evidence_based_weighting

//counts are floats because of isf
case class EventCounts(var strongPositive:Float,
                       var weakPositive:Float,
                       var neutral:Float,
                       var weakNegative:Float,
                       var strongNegative:Float) {

}

object EventCounts {
  def from(counts: EventOccurrenceStatistics) = {
    val summedScores = counts.summedScores.get
    EventCounts(summedScores(0),summedScores(1),summedScores(2),summedScores(3),summedScores(4))
  }
}
