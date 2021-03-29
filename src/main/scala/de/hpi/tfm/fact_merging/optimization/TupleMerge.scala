package de.hpi.tfm.fact_merging.optimization

import de.hpi.tfm.compatibility.graph.fact.IDBasedTupleReference
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.io.DBSynthesis_IOService.{EVALUATION_RESULT_DIR, FIELD_MERGE_RESULT_DIR, createParentDirs}

import java.io.File

case class TupleMerge(clique:Set[IDBasedTupleReference],score:Double) extends JsonWritable[TupleMerge]{

}

object TupleMerge extends JsonReadable[TupleMerge] {
  def loadIncorrectMerges(subdomain:String,methodName: String) = fromJsonObjectPerLineFile(getIncorrectMergeFile(subdomain,methodName).getAbsolutePath)

  def loadCorrectMerges(subdomain:String,methodName: String) = fromJsonObjectPerLineFile(getCorrectMergeFile(subdomain,methodName).getAbsolutePath)


  def getCorrectMergeFile(subdomain:String,methodName: String) = createParentDirs(new File(EVALUATION_RESULT_DIR(subdomain,methodName) + "/correctMerges.json"))
  def getIncorrectMergeFile(subdomain:String,methodName: String) = createParentDirs(new File(EVALUATION_RESULT_DIR(subdomain,methodName) + "/incorrectMerges.json"))


  def getStandardObjectPerLineFiles(subdomain:String,methodName:String) = {
    createParentDirs(new File(FIELD_MERGE_RESULT_DIR(subdomain,methodName))).listFiles()
  }

  def getStandardJsonObjectPerLineFile(subdomain:String,methodName:String,componentFileName: String) = {
    createParentDirs(new File(FIELD_MERGE_RESULT_DIR(subdomain,methodName) + componentFileName + ".json"))
  }
}
