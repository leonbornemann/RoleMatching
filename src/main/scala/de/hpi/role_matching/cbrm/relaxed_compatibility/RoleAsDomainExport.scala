package de.hpi.role_matching.cbrm.relaxed_compatibility

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.{File, PrintWriter}
import java.time.LocalDate

object RoleAsDomainExport extends App {
  private val source: String = args(0)
  GLOBAL_CONFIG.setSettingsForDataSource(source)
  val rolesetDir = args(1)
  val rolesetFiles = new File(rolesetDir).listFiles()
  val traintTimeEnd = LocalDate.parse(args(2))
  val decayThreshold = args(3).toDouble
  val resultDir = new File(args(4))
  val mode = args(5)
  resultDir.mkdirs()
  if(mode == "cbrb"){
    rolesetFiles.foreach(rolesetFile => {
      println(s"Processing $rolesetFile")
      val resultFileWriterIndex = new PrintWriter(resultDir.getAbsolutePath + s"/${rolesetFile.getName}_index")
      val resultFileWriterQuery = new PrintWriter(resultDir.getAbsolutePath + s"/${rolesetFile.getName}_query")
      val roleset = Roleset.fromJsonFile(rolesetFile.getAbsolutePath)
      roleset.getStringToLineageMap
        .values
        .toIndexedSeq
        .map(rl => (rl.id,rl.roleLineage.toRoleLineage.applyDecay(decayThreshold,traintTimeEnd)))
        .sortBy(_._1)
        .foreach{case (k,rl) => {
          val roleAsDomainQuery = rl.toCBRBDomain(k,GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,traintTimeEnd,true)
          roleAsDomainQuery.appendToWriter(resultFileWriterQuery,false,true)
          val roleAsDomainIndex = rl.toCBRBDomain(k,GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,traintTimeEnd,false)
          roleAsDomainIndex.appendToWriter(resultFileWriterIndex,false,true)
        }}
      resultFileWriterQuery.close()
      resultFileWriterIndex.close()
    })
  } else {
    assert(mode=="rm")
    rolesetFiles.foreach(rolesetFile => {
      val resultFileWriter = new PrintWriter(resultDir.getAbsolutePath + s"/${rolesetFile.getName}")
      val roleset = Roleset.fromJsonFile(rolesetFile.getAbsolutePath)
      roleset.getStringToLineageMap
        .values
        .toIndexedSeq
        .map(rl => (rl.id,rl.roleLineage.toRoleLineage.applyDecay(decayThreshold,traintTimeEnd)))
        .sortBy(_._1)
        .foreach{case (k,rl) => {
          val roleAsDomain = rl.toRoleAsDomain(k,GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,traintTimeEnd)
          roleAsDomain.appendToWriter(resultFileWriter,false,true)
        }}
      resultFileWriter.close()
    })
  }
}