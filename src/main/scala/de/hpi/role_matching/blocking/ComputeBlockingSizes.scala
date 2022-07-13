package de.hpi.role_matching.blocking

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate

object ComputeBlockingSizes extends App {
  GLOBAL_CONFIG.setSettingsForDataSource(args(0))
  val rolesetDir = new File(args(1))
  val trainTimeEnd = LocalDate.parse(args(2))
  println("dataset,EM,QSM,TSM,VSM")
  rolesetDir.listFiles().foreach(f => {
    val roleset = Roleset.fromJsonFile(f.getAbsolutePath)
    val emCount = new ExactSequenceMatchBlocking(roleset, trainTimeEnd).getMatchCount()
    val csmCount = new ChangeSequenceBlocking(roleset, trainTimeEnd).getMatchCount()
    val vsCount = new ValueSetBlocking(roleset, trainTimeEnd).getMatchCount()
    val tsmCount = new TransitionSetBlocking(roleset,trainTimeEnd).getMatchCount()
    println(f.getName.split("\\.")(0), emCount, csmCount, tsmCount,vsCount)
  })

}
