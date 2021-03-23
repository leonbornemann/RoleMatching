package de.hpi.dataset_versioning.db_synthesis.evaluation

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.{JsonReadable, JsonWritable}
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.evaluation.TupleMergeEvaluationResult.getStandardFile
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

import java.io.File

case class TupleMergeEvaluationResult(var correctNoChange: Int=0,
                                      var incorrectNoChange: Int=0,
                                      var correctWithChange: Int=0,
                                      var incorrectWithChange: Int=0) extends JsonWritable[TupleMergeEvaluationResult] with StrictLogging{

  def accuracyAll = (correctNoChange + correctWithChange) / (correctNoChange+incorrectNoChange+correctWithChange+incorrectWithChange).toDouble

  def accuracyWithChange: Any = correctWithChange / (correctWithChange + incorrectWithChange).toDouble

  def printStats() = {
    logger.debug("Result: " + this.toString)
    logger.debug("Accuracy (all): " + accuracyAll)
    logger.debug(s"Accuracy (with change after ${IOService.STANDARD_TIME_FRAME_END}): " + accuracyWithChange)
  }


  def writeToStandardFile(methodName: String) = {
    toJsonFile(getStandardFile(methodName))
  }


  def updateCount(res: Option[ValueLineage]) = {
    val interesting = res.exists(_.lineage.lastKey.isAfter(IOService.STANDARD_TIME_FRAME_END))
    if(res.isDefined && interesting) correctWithChange +=1
    else if(res.isDefined && !interesting) correctNoChange+=1
    else if(!res.isDefined && interesting) incorrectWithChange += 1
    else if(!res.isDefined && !interesting) incorrectNoChange += 1
  }
}
object TupleMergeEvaluationResult extends JsonReadable[TupleMergeEvaluationResult]{
  def getStandardFile(methodName:String) = {
    new File(DBSynthesis_IOService.EVALUATION_RESULT_DIR(methodName) + "/tupleMergeEvaluationResult.json")
  }
}
