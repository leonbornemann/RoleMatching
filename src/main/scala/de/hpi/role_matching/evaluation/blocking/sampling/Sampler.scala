package de.hpi.role_matching.evaluation.blocking.sampling

import de.hpi.role_matching.data.{RoleLineage, RoleLineageWithID, RoleMatchCandidate, RoleMatchCandidateIds}
import de.hpi.role_matching.evaluation.blocking.ground_truth.RoleMatchStatistics

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.util.Random

abstract class Sampler(outputDir: String, seed: Long,trainTimeEnd:LocalDate) {

  val random = new Random(seed)
  new File(outputDir).mkdirs()

  def serializeSampleAndStats(dsName:String,
                              sample: collection.Set[RoleMatchCandidateIds],
                              roleMap:Map[String,RoleLineage]) = {
    val outFileEdges = new PrintWriter(outputDir + "/" + dsName + ".json")
    val outFileStats = new PrintWriter(outputDir + "/" + dsName + ".csv")
    val DECAY_THRESHOLD = 0.57
    val DECAY_THRESHOLD_SCB = 0.5
    RoleMatchStatistics.appendSchema(outFileStats)
    sample.foreach(e => {
      serializeMatch(dsName, roleMap, outFileEdges, outFileStats, e)
    })
    outFileStats.close()
    outFileEdges.close()
  }

  def serializeMatch(dsName: String, roleMap: Map[String, RoleLineage], outFileEdges: PrintWriter, outFileStats: PrintWriter, e: RoleMatchCandidateIds) = {
    e.appendToWriter(outFileEdges, false, true)
    val simpleEdge = RoleMatchCandidate(RoleLineageWithID(e.v1, roleMap(e.v1).toSerializationHelper),
      RoleLineageWithID(e.v2, roleMap(e.v2).toSerializationHelper))
    val stats = new RoleMatchStatistics(dsName, simpleEdge, false, trainTimeEnd)
    stats.appendStatRow(outFileStats)
  }
}
