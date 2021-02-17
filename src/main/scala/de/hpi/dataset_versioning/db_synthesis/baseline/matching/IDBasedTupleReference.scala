package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier

case class IDBasedTupleReference(associationID: DecomposedTemporalTableIdentifier, rowIndex: Int) {

  def toTupleReference[A](association: TemporalDatabaseTableTrait[A]) = {
    assert(association.getUnionedOriginalTables.size==1 && association.getUnionedOriginalTables.head == associationID)
    TupleReference(association,rowIndex)
  }

}
