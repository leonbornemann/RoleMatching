package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait

class TableUnionMatch[A](val firstMatchPartner:TemporalDatabaseTableTrait[A],
                      val secondMatchPartner:TemporalDatabaseTableTrait[A],
                      val schemaMapping:Option[collection.Map[Set[AttributeLineage], Set[AttributeLineage]]],
                      val score:Int,
                      val isHeuristic:Boolean,
                      val tupleMapping:Option[TupleSetMatching[A]]) {

  def getNonOverlappingElement[A](other: TableUnionMatch[A]) = {
    assert(hasParterOverlap(other))
    assert(!(firstMatchPartner == other.firstMatchPartner &&
      secondMatchPartner == other.secondMatchPartner ||
      secondMatchPartner == other.firstMatchPartner &&
      firstMatchPartner == other.secondMatchPartner))
    if(firstMatchPartner == other.firstMatchPartner || firstMatchPartner == other.secondMatchPartner)
      secondMatchPartner
    else
      firstMatchPartner
  }


  def hasParterOverlap[A](other: TableUnionMatch[A]): Boolean = {
    firstMatchPartner == other.firstMatchPartner ||
      firstMatchPartner == other.secondMatchPartner ||
      secondMatchPartner == other.firstMatchPartner ||
      secondMatchPartner == other.secondMatchPartner
  }

}
