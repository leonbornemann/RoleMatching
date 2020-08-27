package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.db_synthesis.sketches.TemporalTableSketch

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TupleSetMatching(val sketchA: TemporalTableSketch,
                       val sketchB: TemporalTableSketch,
                       val unmatchedTupleIndicesA: mutable.HashSet[Int] = mutable.HashSet(),
                       val unmatchedTupleIndicesB: mutable.HashSet[Int] = mutable.HashSet(),
                       val matchedTuples: ArrayBuffer[TupleMatching] = ArrayBuffer()) {

  def totalScore = matchedTuples.map(_.matchScore).sum

  def is1to1Matching: Boolean ={
    val tupleToMentionCountA = mutable.HashMap[Int,Int]()
    val tupleToMentionCountB = mutable.HashMap[Int,Int]()
    unmatchedTupleIndicesA.foreach(i => {
      val curCount = tupleToMentionCountA.getOrElse(i,0)
      tupleToMentionCountA(i) = curCount +1
    })
    unmatchedTupleIndicesB.foreach(i => {
      val curCount = tupleToMentionCountB.getOrElse(i,0)
      tupleToMentionCountB(i) = curCount +1
    })
    matchedTuples.foreach(m => {
      val curCountA = tupleToMentionCountA.getOrElse(m.tupleIndexA,0)
      tupleToMentionCountA(m.tupleIndexA) = curCountA +1
      val curCountB = tupleToMentionCountB.getOrElse(m.tupleIndexB,0)
      tupleToMentionCountB(m.tupleIndexB) = curCountB +1
    })
    tupleToMentionCountA.forall(_._2==1) && tupleToMentionCountB.forall(_._2==1)
  }

  def ++=(curMatching: TupleSetMatching) = {
    unmatchedTupleIndicesA ++=curMatching.unmatchedTupleIndicesA
    unmatchedTupleIndicesB ++=curMatching.unmatchedTupleIndicesB
    matchedTuples ++=curMatching.matchedTuples
  }
}
