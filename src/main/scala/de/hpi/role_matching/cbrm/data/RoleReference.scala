package de.hpi.role_matching.cbrm.data

@SerialVersionUID(3L)
case class RoleReference(roles: NonCaseClassRoleset, rowIndex: Int) extends Comparable[RoleReference] with Serializable {
  val nonWildCardChangePointsInTrainPeriod = getProjectedRole.allNonWildcardTimestamps.toSet

  def getProjectedRole = roles.posToProjectedRoleLineage(rowIndex)


  def getRole = {
    roles.posToRoleLineage(rowIndex)
  }

  override def compareTo(o: RoleReference): Int = {
    rowIndex.compareTo(o.rowIndex)
  }
}
