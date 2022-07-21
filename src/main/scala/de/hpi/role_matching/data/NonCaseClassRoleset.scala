package de.hpi.role_matching.data

import de.hpi.util.GLOBAL_CONFIG

import java.time.LocalDate

class NonCaseClassRoleset(val roleset:Roleset,trainTimeEnd:LocalDate) {

  //val v1LineageTrain = e.v1.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,graph.smallestTrainTimeEnd)
  //        val v2LineageTrain = e.v2.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,graph.smallestTrainTimeEnd)
  //        val evidence = v1LineageTrain.getOverlapEvidenceCount(v2LineageTrain)
  //        evidence>0
  val posToProjectedRoleLineage = roleset.posToRoleLineage.map{case (i,rl) =>
    (i,rl.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))
  }

  def posToRoleLineage(rowIndex: Int) = roleset.posToRoleLineage(rowIndex)

  def wildcardValues = roleset.wildcardValues

}
