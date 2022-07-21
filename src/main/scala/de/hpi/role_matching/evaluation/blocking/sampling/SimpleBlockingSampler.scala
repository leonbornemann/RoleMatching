package de.hpi.role_matching.evaluation.blocking.sampling

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.data.{RoleLineage, RoleMatchCandidateIds, RoleMatchCandidateWithIntegerIDs, Roleset}
import de.hpi.util.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable

class SimpleBlockingSampler(rolesetDir: File,
                            outputDir: String,
                            trainTimeEnd:LocalDate,
                            seed:Long,
                            minVACount:Int,
                            minDVACount:Int,
                            sampleGroundTruth:Boolean,
                            nonGroundTruthSampleCount:Option[Int]) extends Sampler(outputDir,seed,trainTimeEnd) with StrictLogging{

  def getBlockingAtTime(roleMap: Map[String, RoleLineage], ts: LocalDate) = {
    val blocking = new TimestampBlocking(roleMap,ts)
    logger.debug(s"Finished blocking at $ts")
    blocking
  }

  def getBlockingOfPair(blockings: collection.mutable.TreeMap[Long,TimestampBlocking], chosenPair: Long) = {
    blockings.maxBefore(chosenPair).get
  }

  def getSampleForCompatibilityRanges(blockings: collection.mutable.TreeMap[Long,TimestampBlocking],
                                      roleMap: Map[String, RoleLineage],
                                      sampleTargetCount:SampleTargetCount) = {
    val sample = collection.mutable.HashSet[RoleMatchCandidateIds]()
    val totalPairCount = blockings.values.map(_.nPairsInBlocking).sum
    while(sampleTargetCount.needsMoreSamples){
      //now we need to draw a weighted sample:
      val chosenPair = random.nextLong(totalPairCount)
      val (startBlockingID,blocking) = blockings.maxBefore(chosenPair+1).get
      val (startBlockID,block) = blocking.getBlockOfPair(chosenPair-startBlockingID)
      val (v1,v2) = block.getPair(chosenPair-startBlockingID-startBlockID)
      if(v1 == v2){
        println()
      }
      addSampleIfValid(roleMap, Some(sampleTargetCount), sample, v1, v2)
      logger.debug(s"Added new Sample - Size: ${sample.size}, cur sampletarget count: $sampleTargetCount")
    }
    sample
  }

  def passedMinDVAFilter(r1: RoleLineage, r2: RoleLineage) = {
    r1.exactDistinctMatchWithoutWildcardCount(r2,trainTimeEnd) >= minDVACount
  }

  def passesMinVAFilter(rl1: RoleLineage, rl2: RoleLineage): Boolean = {
    rl1.exactMatchWithoutWildcardCount(rl2,trainTimeEnd,true) >= minVACount
  }

  def addSampleIfValid(roleMap: Map[String, RoleLineage],
                       sampleTargetCount: Option[SampleTargetCount],
                       sample: mutable.HashSet[RoleMatchCandidateIds],
                       v1: String,
                       v2: String) = {
    //dva filter:
    val rl1 = roleMap(v1)
    if(!roleMap.contains(v2)){
      println()
    }
    val rl2 = roleMap(v2)
    if(passedMinDVAFilter(rl1,rl2) && passesMinVAFilter(rl1,rl2)){
      val e = if (v1 < v2) RoleMatchCandidateIds(v1, v2) else RoleMatchCandidateIds(v2, v1)
      //get compatibility percentage:
      val percentage = roleMap(v1).getCompatibilityTimePercentage(roleMap(v2), trainTimeEnd)
      if (! sample.contains(e) && (sampleTargetCount.isEmpty ||  sampleTargetCount.get.stillNeeds(percentage))) {
        sample.add(e)
        if(sampleTargetCount.isDefined)
          sampleTargetCount.get.reduceNeededCount(percentage)
      }
    }
  }

  def getUniformSample(blockings: mutable.TreeMap[Long, TimestampBlocking],
                       roleMap: Map[String, RoleLineage],
                       targetCount: Int): collection.Set[RoleMatchCandidateIds] = {
    val sample = collection.mutable.HashSet[RoleMatchCandidateIds]()
    val totalPairCount = blockings.values.map(_.nPairsInBlocking).sum
    while(sample.size<targetCount){
      //now we need to draw a weighted sample:
      val chosenPair = random.nextLong(totalPairCount)
      val (startBlockingID,blocking) = blockings.maxBefore(chosenPair+1).get
      val (startBlockID,block) = blocking.getBlockOfPair(chosenPair-startBlockingID)
      val (v1,v2) = block.getPair(chosenPair-startBlockingID-startBlockID)
      addSampleIfValid(roleMap, None, sample, v1, v2)
      if(sample.size%100==0){
        logger.debug(s"Current sample Size: ${sample.size}/$targetCount ( ${100*sample.size/targetCount.toDouble}%")
      }
    }
    sample
  }

  def runSampling() = {
    rolesetDir.listFiles().foreach{ f =>
      logger.debug(s"Processing ${f}")
      val roleset = Roleset.fromJsonFile(f.getAbsolutePath)
      val dsName = f.getName.split("\\.")(0)
      val sampleTargetCount = SampleTargetCount(100,100,100)
      val stringToPosition = roleset.positionToRoleLineage.map{case (pos,rl) => (rl.id,pos)}
      val roleMap = roleset.getStringToLineageMap.map{case (id,rl) => (id,rl.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
      val timestamps = (GLOBAL_CONFIG.STANDARD_TIME_FRAME_START.toEpochDay to trainTimeEnd.toEpochDay by GLOBAL_CONFIG.granularityInDays)
        .map(l => LocalDate.ofEpochDay(l))
        .toSet
        .toIndexedSeq
        .sorted
      val blockings = timestamps
        .map(ts => getBlockingAtTime(roleMap,ts))
        .filter(_.nPairsInBlocking>0)
        .toIndexedSeq
      //draw sample:
      val blockingsAsTreeMap = collection.mutable.TreeMap[Long,TimestampBlocking]()
      var curSum:Long = 0
      blockings.foreach(b => {
        blockingsAsTreeMap.put(curSum,b)
        curSum +=b.nPairsInBlocking
      })
      if(sampleGroundTruth){
        val sample = getSampleForCompatibilityRanges(blockingsAsTreeMap,roleMap,sampleTargetCount)
        val outFile = new PrintWriter(outputDir + "/" + f.getName)
        val outFileSimpleEdge = new PrintWriter(outputDir + "/" + f.getName + "_simpleEdge.json")
        sample.foreach(e => {
          if(e.v1==e.v2){
            println()
          }
          val roleMatch = RoleMatchCandidateWithIntegerIDs(Set(e.v1,e.v2).map(s => stringToPosition(s)),1.0)
          roleMatch.appendToWriter(outFile,false,true)
          e.appendToWriter(outFileSimpleEdge,false,true)
        })
        outFile.close()
        outFileSimpleEdge.close()
      } else {
        val sample = getUniformSample(blockingsAsTreeMap,roleMap,nonGroundTruthSampleCount.get)
        serializeSampleAndStats(dsName, sample,roleMap)
      }

    }
  }

}
