package de.hpi.role_matching.cbrm.relaxed_blocking

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}

import java.io.File
import java.time.LocalDate
import scala.util.Random

class RelaxedBlocker(roleset: Roleset,trainTimeEnd:LocalDate, resultDir: File, targetPercentage: Double) {

  val identifiedLineages = roleset
    .positionToRoleLineage
    .toIndexedSeq
    .sortBy(_._1)
  val lineages = identifiedLineages
    .map(t => {
      val newLineage = t._2.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd)
      RoleLineageWithID(t._2.id,newLineage.toSerializationHelper)
    })

  val references = RoleLineageWithID.toReferences(lineages,trainTimeEnd)

  def executeBlocking() = {
    val rootNode = new RelaxedBlockingNode(references,trainTimeEnd,targetPercentage,new Random(13))
    println(rootNode.prunable)
    println(rootNode.nonPrunable)
    println(lineages.size)
  }

}
