package de.hpi.role_matching.evaluation.semantic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.RoleGroup
import de.hpi.role_matching.cbrm.data.{ReservedChangeValues, RoleLineage, Roleset}
import de.hpi.role_matching.cbrm.sgcp.RoleMerge

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable
import scala.io.Source
import scala.util.Random

class SimpleBlockingSampler(rolesetDir: File,
                            outputDir: String,
                            trainTimeEnd:LocalDate,
                            seed:Long,
                            minVACount:Int,
                            minDVACount:Int) extends StrictLogging{

  def getBlockingAtTime(roleMap: Map[String, RoleLineage], ts: LocalDate) = {
    val blocking = new TimestampBlocking(roleMap,ts)
    blocking
  }
  val random = new Random(seed)
  new File(outputDir).mkdirs()

  def getBlockingOfPair(blockings: collection.mutable.TreeMap[Long,TimestampBlocking], chosenPair: Long) = {
    blockings.maxBefore(chosenPair).get
  }

  def getSample(blockings: collection.mutable.TreeMap[Long,TimestampBlocking],
                roleMap: Map[String, RoleLineage],
                sampleTargetCount:SampleTargetCount) = {
    val sample = collection.mutable.HashSet[SimpleCompatbilityGraphEdgeID]()
    val totalPairCount = blockings.values.map(_.nPairsInBlocking).sum
    while(sampleTargetCount.needsMoreSamples){
      //now we need to draw a weighted sample:
      val chosenPair = random.nextLong(totalPairCount)
      val (startBlockingID,blocking) = blockings.maxBefore(chosenPair+1).get
      val (startBlockID,block) = blocking.getBlockOfPair(chosenPair-startBlockingID)
      val (v1,v2) = block.getPair(chosenPair-startBlockingID-startBlockID)
      addSampleIfValid(roleMap, sampleTargetCount, sample, v1, v2)
      logger.debug(s"Added new Sample - Size: ${sample.size}, cur sampletarget count: $sampleTargetCount")
    }
    sample
  }

  def passedMinDVAFilter(r1: RoleLineage, r2: RoleLineage) = {
    r1.exactDistinctMatchWithoutWildcardCount(r2,trainTimeEnd) >= minDVACount
  }

  def passesMinVAFilter(rl1: RoleLineage, rl2: RoleLineage): Boolean = {
    rl1.exactMatchWithoutWildcardCount(rl2,trainTimeEnd) >= minVACount
  }

  def addSampleIfValid(roleMap: Map[String, RoleLineage],
                       sampleTargetCount: SampleTargetCount,
                       sample: mutable.HashSet[SimpleCompatbilityGraphEdgeID],
                       v1: String,
                       v2: String) = {
    //dva filter:
    val rl1 = roleMap(v1)
    val rl2 = roleMap(v2)
    if(passedMinDVAFilter(rl1,rl2) && passesMinVAFilter(rl1,rl2)){
      val e = if (v1 < v2) SimpleCompatbilityGraphEdgeID(v1, v2) else SimpleCompatbilityGraphEdgeID(v2, v1)
      //get compatibility percentage:
      val percentage = roleMap(v1).getCompatibilityTimePercentage(roleMap(v2), trainTimeEnd)
      if (sampleTargetCount.stillNeeds(percentage) && !sample.contains(e)) {
        sample.add(e)
        sampleTargetCount.reduceNeededCount(percentage)
      }
    }
  }

  def runSampling() = {
    rolesetDir.listFiles().foreach{ f =>
      logger.debug(s"Processing ${f}")
      val roleset = Roleset.fromJsonFile(f.getAbsolutePath)
      val dsName = f.getName.split("\\.")(0)
      val sampleTargetCount = SampleTargetCount(100,100,100)
      val stringToPosition = roleset.positionToRoleLineage.map{case (pos,rl) => (rl.id,pos)}
      //get rid of artificial wildcards:
      val roleMap = roleset.getStringToLineageMap.map{case (id,rl) => (id,rl.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
      val timestamps = (GLOBAL_CONFIG.STANDARD_TIME_FRAME_START.toEpochDay to trainTimeEnd.toEpochDay by GLOBAL_CONFIG.granularityInDays)
        .map(l => LocalDate.ofEpochDay(l))
        .toSet
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
      val sample = getSample(blockingsAsTreeMap,roleMap,sampleTargetCount)
      val outFile = new PrintWriter(outputDir + "/" + f.getName)
      val outFileSimpleEdge = new PrintWriter(outputDir + "/" + f.getName + "_simpleEdge.json")
      sample.foreach(e => {
        val roleMatch = RoleMerge(Set(e.v1,e.v2).map(s => stringToPosition(s)),1.0)
        roleMatch.appendToWriter(outFile,false,true)
        e.appendToWriter(outFileSimpleEdge,false,true)
      })
      outFile.close()
      outFileSimpleEdge.close()
    }
  }
}
