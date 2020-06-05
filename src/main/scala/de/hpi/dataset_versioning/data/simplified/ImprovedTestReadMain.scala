package de.hpi.dataset_versioning.data.simplified

object ImprovedTestReadMain extends App {
  val outFile = "/home/leon/data/dataset_versioning/socrata/fromServer/simplifiedData/2019-11-01/test-test_colsAndRowsMatched.json?"
  val improvedParsedMatched = RelationalDataset.fromJsonFile(outFile)
  val improvedParsed = RelationalDataset.fromJsonFile("/home/leon/data/dataset_versioning/socrata/fromServer/simplifiedData/2019-11-01/test-test.json?")
  val row1PreMatching = improvedParsed.rows(0).fields
  val row1PostMatching = improvedParsedMatched.rows(0).fields
  assert(row1PostMatching == row1PreMatching)
  println()
}
