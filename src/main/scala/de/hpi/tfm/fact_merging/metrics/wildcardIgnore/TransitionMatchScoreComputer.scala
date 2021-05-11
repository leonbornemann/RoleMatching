package de.hpi.tfm.fact_merging.metrics.wildcardIgnore

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.data.socrata.change.temporal_tables.time.TimeInterval
import de.hpi.tfm.data.tfmp_input.table.TemporalFieldTrait
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.TransitionHistogramMode.TransitionHistogramMode
import de.hpi.tfm.fact_merging.metrics.wildcardIgnore.WildcardIgnoreHistogramBasedComputer.buildTransitionHistogram

class TransitionMatchScoreComputer[A](f1: TemporalFieldTrait[A],
                                      f2: TemporalFieldTrait[A],
                                      TIMESTAMP_RESOLUTION_IN_DAYS:Long,
                                      histogramMode: TransitionHistogramMode,
                                      hist1:Map[ValueTransition[A], IndexedSeq[TimeInterval]],
                                      hist2:Map[ValueTransition[A], IndexedSeq[TimeInterval]])
  extends WildcardIgnoreHistogramBasedComputer[A](f1,f2,TIMESTAMP_RESOLUTION_IN_DAYS,histogramMode,hist1,hist2){

  def this(f1: TemporalFieldTrait[A],
           f2: TemporalFieldTrait[A],
           TIMESTAMP_RESOLUTION_IN_DAYS:Long,
           transitionHistogramMode:TransitionHistogramMode) {
    this(f1,
      f2,
      TIMESTAMP_RESOLUTION_IN_DAYS,
      transitionHistogramMode,
      buildTransitionHistogram(f1,transitionHistogramMode,TIMESTAMP_RESOLUTION_IN_DAYS),
      buildTransitionHistogram(f2,transitionHistogramMode,TIMESTAMP_RESOLUTION_IN_DAYS))
  }

  def overlaps(ti1: TimeInterval, ti2: TimeInterval): Boolean = {
    ti1.intersect(ti2).isDefined
  }

  def computeScore(): Double = {
    val denominator: Int = getDenominator
    val chosenFromIntervals2 = collection.mutable.HashSet[TimeInterval]()
    val nominator = hist1.keySet.intersect(hist2.keySet)
      //.filter(t => !ignoreNonChangeTransitions || t.prev!=t.after)
      .map(k => {
        val intervals1 = hist1(k)
        val matchesForThis = intervals1.map(ti1 => {
          //find all matches in intervals2:
          val newlyChosen = hist2(k).filter(ti2 =>  !chosenFromIntervals2.contains(ti2) && overlaps(ti1,ti2))
          chosenFromIntervals2 ++= newlyChosen
          newlyChosen.size
        }).sum
        matchesForThis
    }).sum
    nominator / denominator.toDouble
  }

  private def getDenominator = {
    //if(!ignoreNonChangeTransitions){
      val hist1IntervalCount = hist1.values.map(_.size).sum
      val hist2IntervalCount = hist2.values.map(_.size).sum
      val denominator = Seq(hist1IntervalCount, hist2IntervalCount).max
      denominator
//    } else {
//      val hist1IntervalCount = hist1
//        .filter(t => t._1.prev!=t._1.after)
//        .values.map(_.size).sum
//      val hist2IntervalCount = hist2
//        .filter(t => t._1.prev!=t._1.after)
//        .values.map(_.size).sum
//      val denominator = Seq(hist1IntervalCount, hist2IntervalCount).max
//      denominator
//    }

  }
}
object TransitionMatchScoreComputer extends StrictLogging{
  logger.debug("score computation here is naively slow - could be optimized")
}
