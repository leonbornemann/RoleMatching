package de.hpi.role_matching.evaluation.semantic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.data.Roleset

import java.io.File
import java.time.LocalDate
import scala.util.Random

class SimpleAllPairSampler(rolesetDir: File, outputDir: String, trainTimeEnd: LocalDate, targetCount: Int, seed:Long)
  extends Sampler(outputDir,seed, trainTimeEnd)  with StrictLogging{

  def runSampling() = {
    rolesetDir.listFiles().foreach { f =>
      logger.debug(s"Processing ${f}")
      val roleset = Roleset.fromJsonFile(f.getAbsolutePath)
      val roleMap = roleset.getStringToLineageMap.map { case (id, rl) => (id, rl.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd)) }
      val roleList = roleMap
        .toIndexedSeq
        .sortBy(_._1)
      val sample = collection.mutable.HashSet[SimpleCompatbilityGraphEdgeID]()
      while(sample.size<targetCount){
        val i = random.nextInt(roleList.size)
        val j = random.nextInt(roleList.size)
        if(i!=j){
          sample.add(SimpleCompatbilityGraphEdgeID(roleList(i)._1,roleList(j)._1))
        }
        if(sample.size%10000==0){
          logger.debug(s"Current sample Size: ${sample.size}/$targetCount ( ${100*sample.size/targetCount.toDouble}%)")
        }
      }
      val dsName = f.getName.split("\\.")(0)
      serializeSampleAndStats(dsName,sample,roleMap)

    }
  }
}
