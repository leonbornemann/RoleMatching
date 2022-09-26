package de.hpi.role_matching.scalability

import de.hpi.role_matching.data.{Roleset, ValueDistribution}
import de.hpi.role_matching.evaluation.RolesetStatisticsPrinter
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.temporal.ChronoUnit
import scala.util.Random

//java -ea -Xmx64g -cp DatasetVersioning-assembly-0.1.jar de.hpi.role_matching.scalability.SyntheticDataGenerationMain /data/changedata/roleMerging/final_experiments/allRolesets/wikipedia/noDecay/football.json 500000 /data/changedata/roleMerging/final_experiments/scalability_experiments/

object SyntheticDataGenerationMain extends App {
  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  //get the distributions of the input dataset:
  private val inputPath = args(0)
  val resultDir = new File(args(1))
  val n = args(2).toInt
  resultDir.mkdirs()
  val rs = Roleset.fromJsonFile(inputPath)
  val valueInLineageDistribution = rs.valueAppearanceInLineageDistribution
  val random = new Random(13)
  //create the data generator:
  val roleDataGenerator = new RoleDataGenerator(rs,valueInLineageDistribution,random)
  val result = roleDataGenerator.generate(n)
  val generated = Roleset.fromRoles(result)
  generated.toJsonFile(new File(resultDir.getAbsolutePath + s"/${n}.json"))
  val statisticsPrinter = new RolesetStatisticsPrinter()
  statisticsPrinter.printSchema()
  statisticsPrinter.printStats(generated,n.toString)
  statisticsPrinter.printStats(rs,"original")
}
