package de.hpi.role_matching.matching

import de.hpi.role_matching.data.{LabelledRoleMatchCandidate, Roleset}

import java.io.{File, PrintWriter}

object DittoTrainEvalTestExtractpr extends App{
  val inputDir="/home/leon/data/dataset_versioning/finalExperiments/dittoData_new_from_server_train_test_split/data/dittoExport/wikipedia/final_with_rm/"
  val outputDir="/home/leon/data/dataset_versioning/finalExperiments/MatchingTrainTestData/"
  val rolesetDir="/home/leon/data/dataset_versioning/finalExperiments/nodecayrolesets_wikipedia/"
  val dsNames=IndexedSeq("politics", "football", "education", "tv_and_film", "military")

  def serializeNewFile(train: Iterator[LabelledRoleMatchCandidate], outFile: String) = {
      new File(outFile)
        .getParentFile
        .mkdirs()
    val pr = new PrintWriter(outFile)
    train.foreach(l => l.appendToWriter(pr,false,true))
    pr.close()
  }

  dsNames.foreach{ dsName =>
    println(s"Processing $dsName")
    val inputFileTrain = inputDir + dsName + ".json.txt_train.txt"
    val inputFileValid = inputDir + dsName + ".json.txt_validation.txt"
    val inputFileTest = inputDir + dsName + ".json.txt_test.txt"
    val rs = Roleset.fromJsonFile(rolesetDir + "/" + dsName + ".json")
    val train = LabelledRoleMatchCandidate.fromDittoFile(inputFileTrain,rs)
    val outputTrainFile = outputDir + s"/$dsName/train.json"
    val outputValidFile = outputDir + s"/$dsName/valid.json"
    val outputTestFile = outputDir + s"/$dsName/test.json"
    serializeNewFile(train,outputTrainFile)
    val valid = LabelledRoleMatchCandidate.fromDittoFile(inputFileValid,rs)
    serializeNewFile(valid,outputValidFile)
    val test = LabelledRoleMatchCandidate.fromDittoFile(inputFileTest,rs)
    serializeNewFile(test,outputTestFile)

  }

}
