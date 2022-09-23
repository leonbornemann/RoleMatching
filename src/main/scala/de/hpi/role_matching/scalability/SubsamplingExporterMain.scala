package de.hpi.role_matching.scalability

import de.hpi.role_matching.data.Roleset

import java.io.File
import scala.util.Random
//java -ea -Xmx64g -cp DatsetVersioning.jar de.hpi.role_matching.scalability.SubsamplingExporterMain /data/changedata/roleMerging/final_experiments/allRolesets/wikipedia/noDecay/football.json
//0.1,0.2,0.3,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0 13 /data/changedata/roleMerging/final_experiments/scalability_experiments/
object SubsamplingExporterMain extends App {
  val inputFile = args(0)
  val downsamplingRatios = args(1).split(";").map(_.toDouble)
  val rs = Roleset.fromJsonFile(inputFile)
  downsamplingRatios.foreach(downsamplingRatio => {
    assert(downsamplingRatio>0.0 && downsamplingRatio <=1.0)
    val random = new Random(args(2).toLong)
    val outputFile = new File(args(3) + new File(inputFile).getName + "_" + downsamplingRatio)
    outputFile.getParentFile.mkdirs()
    rs.subsample(downsamplingRatio,random)
      .toJsonFile(outputFile)
  })

}
