package de.hpi.role_matching.evaluation.semantic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.RoleGroup
import de.hpi.role_matching.cbrm.data.{ReservedChangeValues, RoleLineage, Roleset}
import de.hpi.role_matching.cbrm.sgcp.RoleMerge
import de.hpi.role_matching.evaluation.semantic.SimpleBlockingSamplerMain.{compatibilityGroupDataDir, rolesetDir}

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable
import scala.io.Source
import scala.util.Random

class SimpleBlockingSampler(rolesetDir: File, outputDir: String,trainTimeEnd:LocalDate,seed:Long,
                            sampleTargetCounts:Map[String,SampleTargetCount],
                            existingSampleDir:File,
                            compatibilityGroupDataDirs:Option[IndexedSeq[File]]) extends StrictLogging{

  def getBlockingAtTime(roleMap: Map[String, RoleLineage], ts: LocalDate) = {
    roleMap.groupBy{case (id,r) => r.valueAt(ts)}
      .withFilter{case (v,map) => !GLOBAL_CONFIG.nonInformativeValues.contains(v) && !RoleLineage.isWildcard(v) && map.size>1}
      .map{case (v,map) => (v,map.keySet.toIndexedSeq)}
      .toIndexedSeq
  }
  val random = new Random(seed)
  val sampleSizePerDataset = 500
  new File(outputDir).mkdirs()

  def getSample(blockings: IndexedSeq[IndexedSeq[(Any, IndexedSeq[String])]],
                roleMap: Map[String, RoleLineage],
                sampleTargetCount:SampleTargetCount,
                existingSampleList:Set[SimpleCompatbilityGraphEdgeID]) = {
    val sample = collection.mutable.HashSet[SimpleCompatbilityGraphEdgeID]()
    while(sampleTargetCount.needsMoreSamples){
      val blocking = blockings(random.nextInt(blockings.size))
      val (key,block) = blocking(random.nextInt(blocking.size))
      val i = random.nextInt(block.size)
      var j = i
      assert(block.size>=2)
      while(i==j)
        j = random.nextInt(block.size) //reroll until it is not the same element
      val v1 = block(i)
      val v2 = block(j)
      addSampleIfValid(roleMap, sampleTargetCount, existingSampleList, sample, v1, v2)
      logger.debug(s"Added new Sample - Size: ${sample.size}, cur sampletarget count: $sampleTargetCount")
    }
    sample
  }

  def addSampleIfValid(roleMap: Map[String, RoleLineage],
                               sampleTargetCount: SampleTargetCount,
                               existingSampleList: Set[SimpleCompatbilityGraphEdgeID],
                               sample: mutable.HashSet[SimpleCompatbilityGraphEdgeID],
                               v1: String,
                               v2: String) = {
    val e = if (v1 < v2) SimpleCompatbilityGraphEdgeID(v1, v2) else SimpleCompatbilityGraphEdgeID(v2, v1)
    //get compatibility percentage:
    val percentage = roleMap(v1).getCompatibilityTimePercentage(roleMap(v2), trainTimeEnd)
    if (sampleTargetCount.stillNeeds(percentage) && !existingSampleList.contains(e) && !sample.contains(e)) {
      sample.add(e)
      sampleTargetCount.reduceNeededCount(percentage)
    }
  }

  def useGivenBlocking: Boolean = compatibilityGroupDataDir.isDefined

  def parseRoleCompatibilityGroup(f: File) = {

  }

  def getSampleFromGivenBlocking(roleset: Roleset, dsName:String): collection.Set[SimpleCompatbilityGraphEdgeID] = {
    val dir = new File(compatibilityGroupDataDir.get.find(f => f.getName == dsName).get.getAbsolutePath + "/edges/")
    val groups = dir.listFiles().flatMap(f => RoleGroup.parseRoleCompatibilityGroupsFromFile(f))
    val sample = collection.mutable.HashSet[SimpleCompatbilityGraphEdgeID]()
    while(sample.size<sampleSizePerDataset){
      val sampledGroup = groups(random.nextInt(groups.size))
      val curDraw = sampledGroup.tryDrawSample(random,roleset.getStringToLineageMap,trainTimeEnd)
      if(curDraw.isDefined) sample.add(curDraw.get)
    }
    sample
  }

  def getSampleListFromFile(dsName: String,roleset: Roleset) = {
    Source.fromFile(existingSampleDir.getAbsolutePath + s"/$dsName.csv")
      .getLines()
      .toIndexedSeq
      .tail
      .map(l => (l.split(",")(0).toInt,l.split(",")(1).toInt))
      .map{case (i,j) => {
        val rl1 = roleset.positionToRoleLineage(i)
        val rl2 = roleset.positionToRoleLineage(j)
        if(rl1.id< rl2.id)
          SimpleCompatbilityGraphEdgeID(rl1.id,rl2.id)
        else
          SimpleCompatbilityGraphEdgeID(rl2.id,rl1.id)
      }}
      .toSet

  }

  def runSampling() = {
    rolesetDir.listFiles().foreach{ f =>
      logger.debug(s"Processing ${f}")
      val roleset = Roleset.fromJsonFile(f.getAbsolutePath)
      val dsName = f.getName.split("\\.")(0)
      val existingSampleList = getSampleListFromFile(dsName,roleset)
      val sampleTargetCount = sampleTargetCounts(dsName)
      val stringToPosition = roleset.positionToRoleLineage.map{case (pos,rl) => (rl.id,pos)}
      val sample = if(useGivenBlocking){
        getSampleFromGivenBlocking(roleset,dsName)
      } else {
        //get rid of artificial wildcards:
        val roleMap = roleset.getStringToLineageMap.map{case (id,rl) => (id,rl.roleLineage.toRoleLineage.projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
        val timestamps = (GLOBAL_CONFIG.STANDARD_TIME_FRAME_START.toEpochDay to trainTimeEnd.toEpochDay by GLOBAL_CONFIG.granularityInDays)
          .map(l => LocalDate.ofEpochDay(l))
          .toSet
        val blockings = timestamps
          .map(ts => getBlockingAtTime(roleMap,ts))
          .filter(_.size>0)
          .toIndexedSeq
        //draw sample:
        val sample = getSample(blockings,roleMap,sampleTargetCount,existingSampleList)
        sample
      }

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
