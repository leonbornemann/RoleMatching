package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import de.hpi.data_preparation.socrata.tfmp_input.table.TemporalDatabaseTableTrait
import de.hpi.role_matching.cbrm.compatibility_graph.creation.role_tree
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree

@SerialVersionUID(3L)
case class RoleReference[A](table: TemporalDatabaseTableTrait[A], rowIndex: Int) extends Comparable[RoleReference[A]] with Serializable {
  def toIDBasedTupleReference: IDBasedRoleReference = {
    assert(table.getUnionedOriginalTables.size == 1)
    role_tree.IDBasedRoleReference(table.getUnionedOriginalTables.head, rowIndex)
  }

  import scala.math.Ordering.Implicits._

  def getDataTuple = table.getDataTuple(rowIndex)

  override def compareTo(o: RoleReference[A]): Int = {
    val smaller = (table.getID, rowIndex) < (o.table.getID, o.rowIndex)
    val greater = (table.getID, rowIndex) > (o.table.getID, o.rowIndex)
    if (smaller) -1
    else if (greater) 1
    else 0
  }
}
