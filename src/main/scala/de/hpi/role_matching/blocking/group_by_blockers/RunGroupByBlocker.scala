package de.hpi.role_matching.blocking.group_by_blockers

import de.hpi.role_matching.data.Roleset
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.lang.AssertionError
import java.time.LocalDate

//java -ea -Xmx64g -cp DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.blocking.group_by_blockers.RunGroupByBlocker
// $rolesetFile $groupingAlg $endDateTrainPhase $nthreads $resultDir
object RunGroupByBlocker extends App {
  val rsPath = new File(args(0))
  val groupingAlg = args(1)
  val trainTimeEnd = LocalDate.parse(args(2))
  val nThreads = args(3).toInt
  val resultDir = new File(args(4))
  val timeFactor = args(5).toInt
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia",timeFactor)
  resultDir.mkdirs()
  val rs = Roleset.fromJsonFile(rsPath.getAbsolutePath)
  val grouped = groupingAlg match {
    case "EM" => new EMBlocking(rs,trainTimeEnd)
    case "CQM" => new CQMBlocking(rs,trainTimeEnd)
    case "VSM" => new VSMBlocking(rs,trainTimeEnd)
    case "TSM" => new TSMBlocking(rs,trainTimeEnd)
    case _ => throw new AssertionError("Unknown grouping algorithm")
  }
  grouped.simulatePerfectParallelCandidateSerialization(resultDir,rsPath.getName,1000000,nThreads)

}
