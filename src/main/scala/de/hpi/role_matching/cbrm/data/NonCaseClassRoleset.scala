package de.hpi.role_matching.cbrm.data

class NonCaseClassRoleset(val roleset:Roleset) {
  def posToRoleLineage(rowIndex: Int) = roleset.posToRoleLineage(rowIndex)

  def wildcardValues = roleset.wildcardValues

}
