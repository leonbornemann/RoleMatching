package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.index.ValueLineageIndex
import de.hpi.dataset_versioning.db_synthesis.sketches.DecomposedTemporalTableSketch

class PairwiseTupleMapper(sketchA: DecomposedTemporalTableSketch,
                          sketchB: DecomposedTemporalTableSketch,
                          indexA: ValueLineageIndex,
                          indexB: ValueLineageIndex,
                          mapping:collection.Map[Set[AttributeLineage],Set[AttributeLineage]]) {

  val aColsByID = sketchA.temporalColumnSketches.map(c => (c.attrID,c)).toMap
  val bColsByID = sketchB.temporalColumnSketches.map(c => (c.attrID,c)).toMap

  def mapGreedy() = {
    indexA.index.foreach{case (keyA,tuplesA) => {
      if(indexB.index.contains(keyA)){
        val tuplesB = indexB.index(keyA)
        getBestTupleMatching(tuplesA,tuplesB)
      }
    }}
  }

  def mergeTupleSketches(tupA: Int, tupB: Int) = {
    mapping.map{case (a,b) => {
      val lineagesA = a.toIndexedSeq
        .map(al => aColsByID(al.attrId).fieldLineageSketches(tupA))
        .sortBy(_.lastTimestamp.toEpochDay)
      val lineagesB = b.toIndexedSeq
        .map(al => aColsByID(al.attrId).fieldLineageSketches(tupA))
        .sortBy(_.lastTimestamp.toEpochDay)
      val aMerged = if(lineagesA.size==1) lineagesA.head else lineagesA.reduce((x,y) => x.mergeWithConsistent(y))
    }}
  }

  def getBestTupleMatching(tuplesA: IndexedSeq[Int], tuplesB: IndexedSeq[Int]) = {
    //we do simple pairwise matching here until we find out its a problem
    for(tupA<-tuplesA){
      for(tupB <-tuplesB){
        //apply mapping
        val mergedTuple = mergeTupleSketches(tupA,tupB)

        //TODO: use mapping to get the alignment and count the changes we save everywhere
      }
    }
  }

}
