package de.hpi.role_matching.compatibility.graph.creation

import de.hpi.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.socrata.tfmp_input.table.TemporalDatabaseTableTrait

case class IDBasedTupleReference(associationID: AssociationIdentifier, rowIndex: Int) {

  def toTupleReference[A](association: TemporalDatabaseTableTrait[A]) = {
    assert(association.getUnionedOriginalTables.size == 1 && association.getUnionedOriginalTables.head == associationID)
    TupleReference(association, rowIndex)
  }

  override def toString: String = associationID.compositeID + "_" + rowIndex
}
