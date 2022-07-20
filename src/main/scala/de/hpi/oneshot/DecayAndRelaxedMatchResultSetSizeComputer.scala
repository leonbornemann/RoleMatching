package de.hpi.oneshot

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.Roleset
import de.hpi.role_matching.evaluation.semantic.RoleMatchStatistics

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.io.Source

class DecayAndRelaxedMatchResultSetSizeComputer(originalSampleDir: File, outputDir: File, rolesetDir:File,trainTimeEnd:LocalDate) {

  outputDir.mkdirs()

  val decayThresholds = (0 to 200 )
    .map(i => i / 200.0)
    .reverse

  def process(f: File) = {
    println(s"Processing ${f.getName}")
    val dsName = f.getName.split("\\.")(0)
    val resultPr = new PrintWriter(outputDir.getAbsolutePath + s"/${dsName}.csv")
    resultPr.println("dataset,id1,id2,RM_Gamma,CBRB_Beta,Relaxed_CBRB_Gamma,hasTransitionOverlap")
    val it = Source
      .fromFile(f)
      .getLines()
    //skip header:
    val rs = Roleset.fromJsonFile(rolesetDir.getAbsolutePath + s"/$dsName.json")
    val strToLineage = rs.getStringToLineageMap
      .map{case (s,rl) => (s,rl.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
    it.next()
    it.foreach{ l =>
      val tokens = l.split(",")
      val ds = tokens(0)
      val id1 = tokens(1)
      val id2 = tokens(2)
      val rl1 = strToLineage(id1)
      val rl2 = strToLineage(id2)
      val hasTransitionOverlap = rl1.informativeValueTransitions.intersect(rl2.informativeValueTransitions).size>=1
      if(hasTransitionOverlap){
        val rmScore = rl1.exactMatchesWithoutWildcardPercentage(rl2,trainTimeEnd)
        val cbrbScore = rl1.getCompatibilityTimePercentage(rl2,trainTimeEnd)
        //printResults:
        val decayScore = RoleMatchStatistics.getDecayScore(rl1,rl2,decayThresholds,trainTimeEnd)
        resultPr.println(s"$ds,$id1,$id2,$rmScore,$decayScore,$cbrbScore,$hasTransitionOverlap")
      } else {
        resultPr.println(s"$ds,$id1,$id2,-1.0,-1.0,-1.0,$hasTransitionOverlap")
      }
    }
    resultPr.close()
  }

  def recomputeForOriginalSample() = {
    originalSampleDir
      .listFiles()
      .foreach(f => process(f))
  }

}
