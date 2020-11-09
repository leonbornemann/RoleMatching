package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TupleSetMatching[A](val tableA: TemporalDatabaseTableTrait[A],
                          val tableB: TemporalDatabaseTableTrait[A],
                          unmatchedTupleIndicesA: mutable.HashSet[Int] = mutable.HashSet(),
                          unmatchedTupleIndicesB: mutable.HashSet[Int] = mutable.HashSet(),
                          matchedTuples: ArrayBuffer[TupleMatching] = ArrayBuffer()) {

  if(!is1to1Matching){
    println()
  }
  assert(is1to1Matching)


  def unmatchedTupleIndicesA:collection.Set[Int] = unmatchedTupleIndicesA
  def unmatchedTupleIndicesB:collection.Set[Int] = unmatchedTupleIndicesB
  def matchedTuples:collection.IndexedSeq[TupleMatching] = matchedTuples


  def addUnmatchedTuplesToTableBUnlessAlreadyMatched(tuplesB: collection.Iterable[Int]) = {
    val matchedIndicesB = matchedTuples.map(_.tupleIndexB).toSet
    assert(matchedIndicesB.size==matchedTuples.size)
    addUnmatchedTuplesUnlessAlreadyMatched(tuplesB,unmatchedTupleIndicesB,matchedIndicesB)
  }

  def addUnmatchedTuplesUnlessAlreadyMatched(toAdd: collection.Iterable[Int], collectionToAddTo: mutable.HashSet[Int], matchedIndices: Set[Int]) = {
    collectionToAddTo.addAll(toAdd.toSet.diff(matchedIndices))
  }

  def addUnmatchedTuplesToTableAUnlessAlreadyMatched(tuplesA: collection.Iterable[Int]) = {
    val matchedIndicesA = matchedTuples.map(_.tupleIndexA).toSet
    assert(matchedIndicesA.size==matchedTuples.size)
    addUnmatchedTuplesUnlessAlreadyMatched(tuplesA,unmatchedTupleIndicesA,matchedIndicesA)
  }

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

  def ++=(curMatching: TupleSetMatching[A]) = {
    val byIndex = mutable.HashMap() ++ matchedTuples.map(m => (m.tupleIndexA,m))
    assert(byIndex.size==matchedTuples.size)
    curMatching.matchedTuples.foreach(m => {
      if(byIndex.contains(m.tupleIndexA)){
        val competitor = byIndex(m.tupleIndexA)
        if(competitor.matchScore!=m.matchScore) {
          assert(competitor.tupleIndexB!=m.tupleIndexB)
        }
        if(competitor.matchScore<m.matchScore){
          //swap the two tuples:
          unmatchedTupleIndicesB.addOne(competitor.tupleIndexB)
          byIndex.put(m.tupleIndexA,m)
        } else{
          unmatchedTupleIndicesB.addOne(m.tupleIndexB)
        }
      } else{
        byIndex.put(m.tupleIndexA,m)
        unmatchedTupleIndicesA.remove(m.tupleIndexA)
        unmatchedTupleIndicesB.remove(m.tupleIndexB)
      }
    })
    addUnmatchedTuplesToTableAUnlessAlreadyMatched(curMatching.unmatchedTupleIndicesA)
    addUnmatchedTuplesToTableBUnlessAlreadyMatched(curMatching.unmatchedTupleIndicesB)
    matchedTuples.clear()
    matchedTuples ++= byIndex.values
  }
}
object TupleSetMatching extends StrictLogging{
}
