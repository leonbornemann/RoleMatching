package de.hpi.role_matching.blocking.group_by_blockers

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate

/** *
 * Prints sizes of blocking methods EM,QSM,TSM,VSM
 */
object PrintBlockingResultSetSizesMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetRootDir = new File(args(1))
  val trainTimeEnd = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
  println("dataset,EM,CQM,TSM,VSM")
  rolesetRootDir.listFiles().foreach(rolesetDir => {
    rolesetDir.listFiles().foreach(f => {
      val roleset = Roleset.fromJsonFile(f.getAbsolutePath)
      val emCount = new EMBlocking(roleset, trainTimeEnd).getMatchCount()
      val csmCount = new CQMBlocking(roleset, trainTimeEnd).getMatchCount()
      val vsCount = new VSMBlocking(roleset, trainTimeEnd).getMatchCount()
      val tsmCount = new TSMBlocking(roleset, trainTimeEnd).getMatchCount()
      println(rolesetDir.getName,f.getName.split("\\.")(0), emCount, csmCount, tsmCount, vsCount)
    })
  })


}
