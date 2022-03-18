package de.hpi.role_matching.evaluation.semantic

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdgeID
import de.hpi.role_matching.cbrm.data.{ReservedChangeValues, RoleLineage, Roleset}
import de.hpi.role_matching.evaluation.semantic.SimpleBlockingSamplerMain.rolesetDir

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.util.Random

class SimpleBlockingSampler(rolesetDir: File, outputDir: String,trainTimeEnd:LocalDate,seed:Long) extends StrictLogging{

  def getBlockingAtTime(roleMap: Map[String, RoleLineage], ts: LocalDate) = {
    roleMap.groupBy{case (id,r) => r.valueAt(ts)}
      .withFilter{case (v,map) => !GLOBAL_CONFIG.nonInformativeValues.contains(v) && !RoleLineage.isWildcard(v) && map.size>1}
      .map{case (v,map) => (v,map.keySet.toIndexedSeq)}
      .toIndexedSeq
  }
  val random = new Random(seed)
  val sampleSizePerDataset = 500
  new File(outputDir).mkdirs()

  def getSample(blockings: IndexedSeq[IndexedSeq[(Any, IndexedSeq[String])]]) = {
    val sample = collection.mutable.HashSet[SimpleCompatbilityGraphEdgeID]()
    while(sample.size<sampleSizePerDataset){
      val blocking = blockings(random.nextInt(blockings.size))
      val (key,block) = blocking(random.nextInt(blocking.size))
      val i = random.nextInt(block.size)
      var j = i
      assert(block.size>=2)
      while(i==j)
        j = random.nextInt(block.size) //reroll until it is not the same element
      val v1 = block(i)
      val v2 = block(j)
      val e = if(v1<v2) SimpleCompatbilityGraphEdgeID(v1,v2) else SimpleCompatbilityGraphEdgeID(v2,v1)
      sample.add(e)
      logger.debug(s"Added new Sample - Size: ${sample.size}")
    }
    sample
  }

  def runSampling() = {
    rolesetDir.listFiles().foreach{ f =>
      logger.debug(s"Processing ${f}")
      val rolesets = Roleset.fromJsonFile(f.getAbsolutePath)
      //get rid of artificial wildcards:
      val roleMap = rolesets.getStringToLineageMap.map{case (id,rl) => (id,rl.roleLineage.toRoleLineage.removeDECAYED(ReservedChangeValues.DECAYED).projectToTimeRange(GLOBAL_CONFIG.STANDARD_TIME_FRAME_START,trainTimeEnd))}
      val timestamps = (GLOBAL_CONFIG.STANDARD_TIME_FRAME_START.toEpochDay to trainTimeEnd.toEpochDay by GLOBAL_CONFIG.granularityInDays)
        .map(l => LocalDate.ofEpochDay(l))
        .toSet
      val blockings = timestamps
        .map(ts => getBlockingAtTime(roleMap,ts))
        .filter(_.size>0)
        .toIndexedSeq
      //draw sample:
      val sample = getSample(blockings)
      val outFile = new PrintWriter(outputDir + "/" + f.getName)
      sample.foreach(e => e.appendToWriter(outFile,false,true))
      outFile.close()
    }
  }
}
