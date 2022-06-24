package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.{SimpleCompatbilityGraphEdge, SimpleCompatbilityGraphEdgeID}
import de.hpi.role_matching.cbrm.data.{RoleLineage, RoleLineageWithID}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.util.Random

abstract class Sampler(outputDir: String, seed: Long,trainTimeEnd:LocalDate) {

  val random = new Random(seed)
  new File(outputDir).mkdirs()

  def serializeSampleAndStats(dsName:String,
                                      sample: collection.Set[SimpleCompatbilityGraphEdgeID],
                                      roleMap:Map[String,RoleLineage]) = {
    val outFileEdges = new PrintWriter(outputDir + "/" + dsName + ".json")
    val outFileStats = new PrintWriter(outputDir + "/" + dsName + ".csv")
    val DECAY_THRESHOLD = 0.5
    RoleMatchStatistics.appendSchema(outFileStats)
    sample.foreach(e => {
      e.appendToWriter(outFileEdges, false, true)
      val simpleEdge = SimpleCompatbilityGraphEdge(RoleLineageWithID(e.v1, roleMap(e.v1).toSerializationHelper),
        RoleLineageWithID(e.v2, roleMap(e.v2).toSerializationHelper))
      val stats = new RoleMatchStatistics(dsName, simpleEdge, false, DECAY_THRESHOLD, trainTimeEnd)
      stats.appendStatRow(outFileStats)
    })
    outFileStats.close()
    outFileEdges.close()
  }

}
