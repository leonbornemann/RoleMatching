package de.hpi.role_matching.scalability

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.LocalDate
import java.time.temporal.ChronoUnit

//de.hpi.role_matching.scalability.RolesetWithLongerTimeExport /data/changedata/roleMerging/final_experiments/allRolesets/wikipedia/noDecay/football.json /data/changedata/roleMerging/final_experiments/scalability_experiments_time_scaled/
object RolesetWithLongerTimeExport extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  val input = new File(args(0))
  val factors = IndexedSeq(2,3,4,5,6,7,8,9,10)
  val dates = factors.foreach(i => {
    val ld = LocalDate.parse(GLOBAL_CONFIG.finalWikipediaTrainTimeENd)
    val startOld = GLOBAL_CONFIG.STANDARD_TIME_FRAME_START
    println(startOld.plusDays((ChronoUnit.DAYS.between(startOld,ld)*i).toInt))
  })
  val output = args(1)
  new File(output).getParentFile.mkdirs()
  factors.foreach{ factor =>
    println(factor)
    println(input)
    val rs = Roleset.fromJsonFile(input.getAbsolutePath)
    val outFile = output + "/" + input.getName + s"_$factor"
    rs.exportWithProlongedTime(factor).toJsonFile(new File(outFile))
  }
}
