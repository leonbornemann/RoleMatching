package de.hpi.oneshot

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}

import java.io.File

object RolesetDurationExtender extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val inputRolesetDir = new File(args(1))
  val factors = args(2).split(",").map(_.toDouble)
  val outputDir = args(3)
  new File(outputDir).mkdirs()
  inputRolesetDir.listFiles().foreach(f => {
    println(s"Processing $f")
    val roleset = Roleset.fromJsonFile(f.getAbsolutePath)
    val roleLineageMap = roleset.posToRoleLineage
    factors.foreach(d => {
      val newMap = roleset
        .positionToRoleLineage
        .map{case (pos,rlWithID) =>
          val newLineage = roleLineageMap(pos).toNewTimeScale(d)
//          println(newLineage.lineage.keySet)
//          println(roleLineageMap(pos).lineage.keySet)

          val oldLineageSeq = roleLineageMap(pos).lineage.keySet.toIndexedSeq
          val newLineageSeq = newLineage.lineage.keySet.toIndexedSeq
//          (0 until oldLineageSeq.size).foreach(i => {
//            val duration1 = ChronoUnit.DAYS.between(oldLineageSeq(i),newLineageSeq(i))
//            val duration2 = ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,oldLineageSeq(i))
//            if(duration1!=duration2)
//              println()
//            assert(duration1==duration2)
//          })
          (pos,RoleLineageWithID(rlWithID.id,newLineage.toSerializationHelper))}
      val newRoleset = Roleset(roleset.rolesSortedByID,newMap)
      val resultDir = new File(outputDir + s"/$d/")
      resultDir.mkdirs()
      newRoleset.toJsonFile(new File(resultDir + "/" + f.getName))
    })
  })

}
