package de.hpi.role_matching.evaluation.semantic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.util.Random

class SimpleAllPairSampler(rolesetDir: File, outputDir: String, trainTimeEnd: LocalDate, targetCount: Int, seed:Long)
  extends Sampler(outputDir,seed, trainTimeEnd)  with StrictLogging{

  def isIn95Va2DVA(rl1: RoleLineage, rl2: RoleLineage): Boolean = {
    val daCount = rl1.exactDistinctMatchWithoutWildcardCount(rl2,trainTimeEnd)
    if(daCount>=2){
      val vaCOunt = rl1.exactMatchWithoutWildcardCount(rl2,trainTimeEnd,false)
      vaCOunt>=95
    } else {
      false
    }
  }

  def isIn1VA(rl1: RoleLineage, rl2: RoleLineage): Boolean = {
    val dvaCount = rl1.exactDistinctMatchWithoutWildcardCount(rl2,trainTimeEnd)
    if(dvaCount>=1){
      val daCount = rl1.exactMatchWithoutWildcardCount(rl2,trainTimeEnd,false)
      daCount>=1
    } else {
      false
    }
  }

  def runSampling() = {
    rolesetDir.listFiles().foreach { f =>
      logger.debug(s"Processing ${f}")
      val roleset = Roleset.fromJsonFile(f.getAbsolutePath)
      val roleMap = roleset.getStringToLineageMap.map { case (id, rl) => (id, rl.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START, trainTimeEnd)) }
      val roleList = roleMap
        .toIndexedSeq
        .sortBy(_._1)
      val sample = collection.mutable.HashSet[SimpleCompatbilityGraphEdgeID]()
      var countMissed = 0
      val dsName = f.getName.split("\\.")(0)
      val outFileEdges = new PrintWriter(outputDir + "/" + dsName + ".json")
      val outFileStats = new PrintWriter(outputDir + "/" + dsName + ".csv")
      val DECAY_THRESHOLD = 0.57
      val DECAY_THRESHOLD_SCB = 0.5
      RoleMatchStatistics.appendSchema(outFileStats)
      var logged = false
      while(sample.size<targetCount){
        val i = random.nextInt(roleList.size)
        val j = random.nextInt(roleList.size)
        if(i!=j){
          if(isIn1VA(roleList(i)._2,roleList(j)._2)){
            val roleMatchCandidate = SimpleCompatbilityGraphEdgeID(roleList(i)._1, roleList(j)._1)
            if(!sample.contains(roleMatchCandidate)){
              sample.add(roleMatchCandidate)
              serializeMatch(dsName,roleMap,outFileEdges,outFileStats,DECAY_THRESHOLD,DECAY_THRESHOLD_SCB,roleMatchCandidate)
              outFileStats.flush()
              outFileEdges.flush()
              logged=false
              //logger.debug(s"Dataset $dsName Cur Sample Size:${sample.size} with misses: $countMissed")
            }
          } else {
            countMissed +=1
          }
        }
        if(sample.size%100==0 && !logged){
          logger.debug(s"Current sample Size: ${sample.size}/$targetCount ( ${100*sample.size/targetCount.toDouble}%), missed: $countMissed")
          logged=true
        }
      }
      logger.debug(s"Finshed $dsName with sample size:${sample.size} with misses: $countMissed")
      outFileStats.close()
      outFileEdges.close()
      //serializeSampleAndStats(dsName,sample,roleMap)
    }
  }
}
