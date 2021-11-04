package de.hpi.role_matching.cbrm.data

@SerialVersionUID(3L)
case class RoleReference(roles: Roleset, rowIndex: Int) extends Comparable[RoleReference] with Serializable {

  def getDataTuple = roles.posToRoleLineage(rowIndex)

  override def compareTo(o: RoleReference): Int = {
    rowIndex.compareTo(o.rowIndex)
  }
}
