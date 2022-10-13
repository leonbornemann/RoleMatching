package de.hpi.role_matching.matching

import de.hpi.role_matching.data.{LabelledRoleMatchCandidate, Roleset}
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate

class FeatureTableExporter(inputDir: File, roleset: Roleset, thisOutputDir: File,trainTimeEnd:LocalDate) {

  thisOutputDir.mkdirs()
  val lineageMap = roleset
    .getStringToLineageMap
    .map(t => (t._1,t._2.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd)))
  val featureGenerator = new FeatureGenerator(trainTimeEnd)

  def exportFeatures(f: File) = {
    val resultPR = new PrintWriter(thisOutputDir.getAbsolutePath + "/" + f.getName + ".csv")
    featureGenerator.appendSchema(resultPR)
    LabelledRoleMatchCandidate
      .fromJsonObjectPerLineFile(f.getAbsolutePath)
      .map(l => (l.id1,lineageMap(l.id1),l.id2,lineageMap(l.id2),l.isTrueRoleMatch))
      .foreach{ case(id1,rl1,id2,rl2,label) => featureGenerator.appendFeatures(id1,rl1,id2,rl2,label,resultPR)}
    resultPR.close()
  }

  def exportFeaturesForAll() = {
    inputDir
      .listFiles()
      .foreach(f => exportFeatures(f))
  }


}
