package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.data_preparation.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.data_preparation.socrata.tfmp_input.table.TemporalDatabaseTableTrait

case class IDBasedRoleReference(associationID: AssociationIdentifier, rowIndex: Int) {

  def toTupleReference[A](association: TemporalDatabaseTableTrait[A]) = {
    assert(association.getUnionedOriginalTables.size == 1 && association.getUnionedOriginalTables.head == associationID)
    RoleReference(association, rowIndex)
  }

  override def toString: String = associationID.compositeID + "_" + rowIndex
}
