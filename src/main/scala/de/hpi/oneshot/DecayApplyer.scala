package de.hpi.oneshot

import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}

import java.io.File
import java.time.LocalDate

class DecayApplyer(source: File, decayThreshold: Double, trainTimeEnd:LocalDate, resultFile: String) {

  val lineagesNoDecay = Roleset.fromJsonFile(source.getAbsolutePath)

  def applyDecayToRoleLineage(rl: RoleLineageWithID): RoleLineageWithID = {
    val withDecay = rl.roleLineage.toRoleLineage.applyDecay(decayThreshold,trainTimeEnd)
    RoleLineageWithID(rl.id,withDecay.toSerializationHelper)
  }

  def applyDecay() = {
    val newRoleset = Roleset(lineagesNoDecay.rolesSortedByID,lineagesNoDecay.positionToRoleLineage.map{case (i,rl) => (i,applyDecayToRoleLineage(rl))})
    newRoleset.toJsonFile(new File(resultFile))
  }


}
