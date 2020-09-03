package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.db_synthesis.baseline.{SynthesizedTemporalDatabaseTable, TableUnionMatch}
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

class MetadataBasedHeuristicMatchCalculator extends MatchCalculator{

  def calculateMatch[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A]) = {
    val nonKeyAttrA = tableA.nonKeyAttributeLineages.head
    val nonKeyAttrB = tableB.nonKeyAttributeLineages.head
    assert(tableA.nonKeyAttributeLineages.size==1)
    assert(tableB.nonKeyAttributeLineages.size==1)
    var toReturn = new TableUnionMatch(tableA,tableB,None,0,true,None)
    if(!nonKeyAttrA.nameSet.intersect(nonKeyAttrB.nameSet).isEmpty ){
      //try to see if the times overlap enough
      val valueActiveTimeOverlap = nonKeyAttrA.activeTimeIntervals.intersect(nonKeyAttrB.activeTimeIntervals)
      if(!valueActiveTimeOverlap.isEmpty){
        val keyActiveTimesA = tableA.primaryKey.map(_.activeTimeIntervals).reduce((a, b) => a.union(b))
        val keyActiveTimesB = tableB.primaryKey.map(_.activeTimeIntervals).reduce((a, b) => a.union(b))
        if(!keyActiveTimesA.intersect(keyActiveTimesB).intersect(valueActiveTimeOverlap).isEmpty){
          toReturn = new TableUnionMatch(tableA,tableB,None,Integer.MAX_VALUE,true,None) //TODO: we could plug in the smallest number of rows here if we store that as metadata
        }
      }
    }
    toReturn
  }

}
