package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.index.ValueLineageIndex
import de.hpi.dataset_versioning.db_synthesis.sketches.{TemporalTableSketch, FieldLineageSketch}

import scala.collection.mutable

class PairwiseTupleMapper(sketchA: TemporalTableSketch,
                          sketchB: TemporalTableSketch,
                          indexA: ValueLineageIndex,
                          indexB: ValueLineageIndex,
                          mapping:collection.Map[Set[AttributeLineage],Set[AttributeLineage]]) {

  val aColsByID = sketchA.temporalColumnSketches.map(c => (c.attrID,c)).toMap
  val bColsByID = sketchB.temporalColumnSketches.map(c => (c.attrID,c)).toMap

  def mapGreedy() = {
    val finalMatching = new TupleSetMatching(sketchA,sketchB)
    indexA.index.foreach{case (keyA,tuplesA) => {
      if(indexB.index.contains(keyA)){
        val tuplesB = indexB.index(keyA)
        val curMatching = getBestTupleMatching(tuplesA,tuplesB)
        finalMatching ++= curMatching
      }
    }}
    assert(finalMatching.is1to1Matching)
    finalMatching
  }

  def mergeTupleSketches(mappedFieldLineages:collection.Map[FieldLineageSketch, FieldLineageSketch]) = {
    mappedFieldLineages.map{case (a,b) => a.mergeWithConsistent(b)}.toSeq
  }

  def buildTuples(tupA: Int, tupB: Int) = {
    mapping.map{case (a,b) => {
      val lineagesA = a.toIndexedSeq
        .map(al => aColsByID(al.attrId).fieldLineageSketches(tupA))
        .sortBy(_.lastTimestamp.toEpochDay)
      val lineagesB = b.toIndexedSeq
        .map(al => bColsByID(al.attrId).fieldLineageSketches(tupB))
        .sortBy(_.lastTimestamp.toEpochDay)
      val aConcatenated = if(lineagesA.size==1) lineagesA.head else lineagesA.reduce((x,y) => x.mergeWithConsistent(y))
      val bConcatenated = if(lineagesB.size==1) lineagesB.head else lineagesB.reduce((x,y) => x.mergeWithConsistent(y))
      (aConcatenated,bConcatenated)
    }}
  }

  def getBestTupleMatching(tuplesA: IndexedSeq[Int], tuplesB: IndexedSeq[Int]) = {
    //we do simple pairwise matching here until we find out its a problem
    val tuplesBRemaining = mutable.HashSet() ++ tuplesB
    val unmatchedTuplesA = mutable.HashSet[Int]()
    var tupleMatching = mutable.ArrayBuffer[TupleMatching]()
    for(tupA<-tuplesA){
      var bestMatchScore = 0
      var curBestTupleB = -1
      for(tupB <-tuplesBRemaining){
        //apply mapping
        val mappedFieldLineages = buildTuples(tupA,tupB) // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
        val mergedTuple = mergeTupleSketches(mappedFieldLineages)
        val sizeAfterMerge = mergedTuple.map(_.changeCount).reduce(_+_)
        val sizeBeforeMerge = mappedFieldLineages.map{case (a,b) => a.changeCount+b.changeCount}.reduce(_+_)
        val curScore = sizeBeforeMerge-sizeAfterMerge
        if(curScore>bestMatchScore){
          bestMatchScore = curScore
          curBestTupleB = tupB
        }
      }
      if(curBestTupleB!= -1){
        tupleMatching +=TupleMatching(tupA,curBestTupleB,bestMatchScore)
        tuplesBRemaining.remove(curBestTupleB)
      } else{
        unmatchedTuplesA.add(tupA)
      }
    }
    new TupleSetMatching(sketchA,sketchB,unmatchedTuplesA,tuplesBRemaining,tupleMatching)
  }

}
