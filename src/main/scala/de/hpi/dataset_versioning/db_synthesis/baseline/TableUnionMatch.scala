package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

class TableUnionMatch(val firstMatchPartner:SynthesizedTemporalDatabaseTableSketch,
                      val secondMatchPartner:SynthesizedTemporalDatabaseTableSketch,
                      val schemaMapping:Option[collection.Map[Set[AttributeLineage], Set[AttributeLineage]]],
                      val score:Int,
                      val isHeuristic:Boolean) {

  def getNonOverlappingElement(other: TableUnionMatch) = {
    assert(hasParterOverlap(other))
    if(firstMatchPartner == other.firstMatchPartner || firstMatchPartner == other.secondMatchPartner)
      secondMatchPartner
    else
      firstMatchPartner
  }


  def hasParterOverlap(other: TableUnionMatch): Boolean = {
    firstMatchPartner == other.firstMatchPartner ||
      firstMatchPartner == other.secondMatchPartner ||
      secondMatchPartner == other.firstMatchPartner ||
      secondMatchPartner == other.secondMatchPartner
  }

}
