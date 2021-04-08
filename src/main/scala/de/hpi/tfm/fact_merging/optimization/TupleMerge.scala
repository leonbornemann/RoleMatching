package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.IDBasedTupleReference
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.io.DBSynthesis_IOService.{FIELD_MERGE_RESULT_DIR, createParentDirs}
import de.hpi.tfm.io.Evaluation_IOService.EVALUATION_RESULT_DIR

import java.io.File

case class TupleMerge(clique:Set[IDBasedTupleReference],score:Double) extends JsonWritable[TupleMerge]{

}

object TupleMerge extends JsonReadable[TupleMerge] {
  def loadIncorrectMerges(subdomain:String,methodName: String,graphConfig: GraphConfig) = fromJsonObjectPerLineFile(getIncorrectMergeFile(subdomain,methodName,graphConfig).getAbsolutePath)

  def loadCorrectMerges(subdomain:String,methodName: String,graphConfig: GraphConfig) = fromJsonObjectPerLineFile(getCorrectMergeFile(subdomain,methodName,graphConfig).getAbsolutePath)


  def getCorrectMergeFile(subdomain:String,methodName: String,graphConfig: GraphConfig) = createParentDirs(new File(EVALUATION_RESULT_DIR(subdomain,methodName,graphConfig) + "/correctMerges.json"))
  def getIncorrectMergeFile(subdomain:String,methodName: String,graphConfig: GraphConfig) = createParentDirs(new File(EVALUATION_RESULT_DIR(subdomain,methodName,graphConfig) + "/incorrectMerges.json"))


  def getStandardObjectPerLineFiles(subdomain:String,methodName:String) = {
    createParentDirs(new File(FIELD_MERGE_RESULT_DIR(subdomain,methodName))).listFiles()
  }

  def getStandardJsonObjectPerLineFile(subdomain:String,methodName:String,componentFileName: String) = {
    createParentDirs(new File(FIELD_MERGE_RESULT_DIR(subdomain,methodName) + componentFileName + ".json"))
  }
}
