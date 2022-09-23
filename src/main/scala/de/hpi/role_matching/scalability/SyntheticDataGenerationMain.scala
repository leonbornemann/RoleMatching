package de.hpi.role_matching.scalability

import de.hpi.role_matching.data.{Roleset, ValueDistribution}
import de.hpi.role_matching.evaluation.RolesetStatisticsPrinter
import de.hpi.util.GLOBAL_CONFIG

import java.io.File
import java.time.temporal.ChronoUnit
import scala.util.Random

object SyntheticDataGenerationMain extends App {

  GLOBAL_CONFIG.setSettingsForDataSource("wikipedia")
  //get the distributions of the input dataset:
  private val inputPath = args(0)
  val resultDir = new File(args(1))
  val n = args(2).toInt
  resultDir.mkdirs()
  val rs = Roleset.fromJsonFile(inputPath)
  val densities = rs.posToRoleLineage
    .values
    .map(rl => rl.nonWildcardDuration(GLOBAL_CONFIG.STANDARD_TIME_FRAME_END.plusDays(1)) / ChronoUnit.DAYS.between(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, GLOBAL_CONFIG.STANDARD_TIME_FRAME_END).toDouble)
  private val distinctValueCounts = rs
    .posToRoleLineage
    .values
    .map(_.nonWildCardValues.size)
  val distinctValueCountDistribution = ValueDistribution.fromIterable(distinctValueCounts)
  val densityDistribution = ValueDistribution.fromIterable(densities)
  val valueInLineageDistribution = rs.valueAppearanceInLineageDistribution
  val random = new Random(13)
  //create the data generator:
  val roleDataGenerator = new RoleDataGenerator(distinctValueCountDistribution,densityDistribution,valueInLineageDistribution,random)
  val result = roleDataGenerator.generate(n)
  val generated = Roleset.fromRoles(result)
  generated.toJsonFile(new File(resultDir.getAbsolutePath + s"/${n}.json"))
  val statisticsPrinter = new RolesetStatisticsPrinter()
  statisticsPrinter.printSchema()
  statisticsPrinter.printStats(generated,n.toString)
  statisticsPrinter.printStats(rs,"original")
}
