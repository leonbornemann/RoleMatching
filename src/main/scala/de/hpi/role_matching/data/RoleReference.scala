package de.hpi.role_matching.data

@SerialVersionUID(3L)
case class RoleReference(roles: NonCaseClassRoleset, rowIndex: Int) extends Comparable[RoleReference] with Serializable {
  val nonWildCardChangePointsInTrainPeriod = getProjectedRole.allNonWildcardTimestamps.toSet

  def getProjectedRole = roles.posToProjectedRoleLineage(rowIndex)

  def getRoleID = roles.roleset.positionToRoleLineage(rowIndex).id

  def getRole = {
    roles.posToRoleLineage(rowIndex)
  }

  override def compareTo(o: RoleReference): Int = {
    rowIndex.compareTo(o.rowIndex)
  }
}
