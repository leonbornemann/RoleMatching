package de.hpi.role_matching.blocking.rm

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate

object RoleDomainExportForStretchedTime extends App {
  private val source: String = "wikipedia"
  val rolesetDir = args(0)
  val resultDir = new File(args(1))
  resultDir.mkdirs()
  val rolesetFiles = new File(rolesetDir)
    .listFiles()
  val traintTimeEnds = IndexedSeq("2016-05-07", "2029-09-08", "2043-01-10", "2056-05-13", "2069-09-14", "2083-01-16", "2096-05-19", "2109-09-21", "2123-01-23", "2136-05-26")
    .map(LocalDate.parse(_))
  rolesetFiles
    .sortBy(_.getName)
    .zipWithIndex
    .foreach{case (rolesetFile,i) => {
      println("Processing ",rolesetFile,i)
      val timeFactor = rolesetFile.getName.split("_")(1).toInt
      val traintTimeEnd = traintTimeEnds(i)
      GLOBAL_CONFIG.setSettingsForDataSource(source,timeFactor)
      val resultFileWriter = new PrintWriter(resultDir.getAbsolutePath + s"/${rolesetFile.getName}")
      val roleset = Roleset.fromJsonFile(rolesetFile.getAbsolutePath)
      roleset.getStringToLineageMap
        .values
        .toIndexedSeq
        .map(rl => (rl.id, rl.roleLineage.toRoleLineage))
        .sortBy(_._1)
        .foreach { case (k, rl) => {
          val roleAsDomain = rl.toRoleAsDomain(k, GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, traintTimeEnd)
          roleAsDomain.appendToWriter(resultFileWriter, false, true)
        }
        }
      resultFileWriter.close()
    }}
}
