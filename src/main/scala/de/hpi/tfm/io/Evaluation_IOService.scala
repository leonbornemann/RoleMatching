package de.hpi.tfm.io

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.io.DBSynthesis_IOService.{DB_SYNTHESIS_DIR, createParentDirs}

import java.io.File

object Evaluation_IOService {

  def getEdgeEvaluationFile(subdomain: String, trainGraphConfig: GraphConfig, evaluationGraphConfig: GraphConfig) = {
    createParentDirs(new File(EVALUATION_RESULT_DIR(subdomain,trainGraphConfig) + s"/$evaluationGraphConfig/allEdgesEvaluation.csv"))
  }


  def socrataDir = IOService.socrataDir

  def EVALUATION_DIR(subdomain:String) = createParentDirs(new File(DB_SYNTHESIS_DIR + s"/evaluationResults/$subdomain/")).getAbsolutePath
  def EVALUATION_RESULT_DIR(subdomain:String, graphConfig: GraphConfig) = createParentDirs(new File(EVALUATION_DIR(subdomain) + s"/${graphConfig.toFileNameString}/")).getAbsolutePath
  def EVALUATION_RESULT_DIR_FOR_METHOD(subdomain:String, methodName: String, graphConfig: GraphConfig) = createParentDirs(new File(EVALUATION_DIR(subdomain) + s"/${graphConfig.toFileNameString}/$methodName/")).getAbsolutePath


}
