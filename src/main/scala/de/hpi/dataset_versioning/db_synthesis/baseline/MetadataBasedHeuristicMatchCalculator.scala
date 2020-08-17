package de.hpi.dataset_versioning.db_synthesis.baseline

class MetadataBasedHeuristicMatchCalculator {

  def calculateMatch(tableA: SynthesizedTemporalDatabaseTable, tableB: SynthesizedTemporalDatabaseTable) = {
    val nonKeyAttrA = tableA.nonKeyAttributeLineages.head
    val nonKeyAttrB = tableB.nonKeyAttributeLineages.head
    assert(tableA.nonKeyAttributeLineages.size==1)
    assert(tableB.nonKeyAttributeLineages.size==1)
    var toReturn = new HeuristicMatch(tableA,tableB,0)
    if(!nonKeyAttrA.nameSet.intersect(nonKeyAttrB.nameSet).isEmpty ){
      //try to see if the times overlap enough
      val valueActiveTimeOverlap = nonKeyAttrA.activeTimeIntervals.intersect(nonKeyAttrB.activeTimeIntervals)
      if(!valueActiveTimeOverlap.isEmpty){
        val keyActiveTimesA = tableA.keyAttributeLineages.map(_.activeTimeIntervals).reduce((a, b) => a.union(b))
        val keyActiveTimesB = tableB.keyAttributeLineages.map(_.activeTimeIntervals).reduce((a, b) => a.union(b))
        if(!keyActiveTimesA.intersect(keyActiveTimesB).intersect(valueActiveTimeOverlap).isEmpty){
          toReturn = new HeuristicMatch(tableA,tableB,Integer.MAX_VALUE) //TODO: we could plug in the smallest number of rows here if we store that as metadata
        }
      }
    }
    toReturn
  }

}
