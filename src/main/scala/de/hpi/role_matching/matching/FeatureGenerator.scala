package de.hpi.role_matching.matching

import de.hpi.role_matching.data.RoleLineage
import de.hpi.util.GLOBAL_CONFIG

import java.io.PrintWriter
import java.time.LocalDate

class FeatureGenerator(trainTimeEnd: LocalDate) {


  def property(id:String) = {
    id.split("\\|\\|").last
  }

  def template(id:String) = {
    id.split("\\|\\|").head
  }

  def pageID(id:String) = {
    id.split("\\|\\|")(1)
  }

  val maxFeatureCount = 50

  var schemaCount = 0

  def appendFeatures(id1:String, rl1: RoleLineage,id2:String, rl2: RoleLineage, label: Boolean,resultPR:PrintWriter) = {
    val baseFeatures = IndexedSeq(label,template(id1),template(id2),pageID(id1),pageID(id2),property(id1),property(id2))
      .mkString(",")
    val features1 = rl1.changeFeaturesAsCSVString(trainTimeEnd,maxFeatureCount)
    val features2 = rl2.changeFeaturesAsCSVString(trainTimeEnd,maxFeatureCount)
    val linestr = baseFeatures + "," + features1 + "," + features2
//    if(!(linestr.count(c => c == ',')==schemaCount))
//      println()
//    assert(linestr.count(c => c == ',')==schemaCount)
    resultPR.println(linestr)
  }

  //$template COL EID VAL $pageID COL PID VAL $propertyID
  def appendSchema(resultPR: PrintWriter) = {
    val baseSchema = Seq("label", "template1", "template2", "pageID1", "pageID2", "property1", "property2")
    val schema1 = (0 until 50)
      .map(i => s"timestampOfChange_${i}_r1,valueOfChange${i}_r1,durationOfChange${i}_r1")
    val schema2 = (0 until 50)
      .map(i => s"timestampOfChange_${i}_r2,valueOfChange${i}_r2,durationOfChange${i}_r2")
    val schema = baseSchema ++ schema1 ++ schema2
    val schemaStr = schema.mkString(",")

    // GLOBAL_CONFIG.STANDARD_TIME_RANGE
//      .filter(ld => !ld.isAfter(trainTimeEnd))
//      .sorted
//      .map(_.toString)
//      .mkString(",")
    schemaCount = schemaStr.count(c => c==',')
    resultPR.println(schemaStr)
  }

}
