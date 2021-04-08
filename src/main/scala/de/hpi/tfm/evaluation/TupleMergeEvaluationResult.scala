package de.hpi.tfm.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.evaluation.TupleMergeEvaluationResult.getStandardFile
import de.hpi.tfm.io.DBSynthesis_IOService.createParentDirs
import de.hpi.tfm.io.{DBSynthesis_IOService, Evaluation_IOService, IOService}

import java.io.File

case class TupleMergeEvaluationResult(var correctNoChange: Int=0,
                                      var incorrectNoChange: Int=0,
                                      var correctWithChange: Int=0,
                                      var incorrectWithChange: Int=0) extends JsonWritable[TupleMergeEvaluationResult] with StrictLogging{
  def total: Int = correctNoChange+incorrectNoChange+correctWithChange+incorrectWithChange


  def accuracyAll = (correctNoChange + correctWithChange) / total.toDouble

  def accuracyWithChange: Any = correctWithChange / (correctWithChange + incorrectWithChange).toDouble

  def printStats() = {
    logger.debug("Result: " + this.toString)
    logger.debug("Accuracy (all): " + accuracyAll)
    logger.debug(s"Accuracy (with change after ${IOService.STANDARD_TIME_FRAME_END}): " + accuracyWithChange)
  }


  def writeToStandardFile(subdomain:String,methodName: String,graphConfig: GraphConfig) = {
    toJsonFile(getStandardFile(subdomain,methodName,graphConfig))
  }


  def checkValidityAndUpdateCount(toCheck:IndexedSeq[FactLineage]) = {
    val res = FactLineage.tryMergeAll(toCheck)
    val interesting = toCheck.exists(_.lineage.lastKey.isAfter(IOService.STANDARD_TIME_FRAME_END))
    if(res.isDefined && interesting)
      correctWithChange +=1
    else if(res.isDefined && !interesting)
      correctNoChange+=1
    else if(!res.isDefined && interesting)
      incorrectWithChange += 1
    else if(!res.isDefined && !interesting) {
      incorrectNoChange += 1
    }
    res.isDefined
  }


}
object TupleMergeEvaluationResult extends JsonReadable[TupleMergeEvaluationResult]{
  def loadFromStandardFile(subdomain:String,methodName:String,graphConfig: GraphConfig) = fromJsonFile(getStandardFile(subdomain,methodName,graphConfig).getAbsolutePath)

  def getStandardFile(subdomain:String,methodName:String,graphConfig: GraphConfig) = {
    createParentDirs(new File(Evaluation_IOService.EVALUATION_RESULT_DIR_FOR_METHOD(subdomain,methodName,graphConfig) + "/tupleMergeEvaluationResult.json"))
  }
}
