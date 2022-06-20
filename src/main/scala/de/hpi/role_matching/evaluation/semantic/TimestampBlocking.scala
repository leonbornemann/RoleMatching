package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.data.RoleLineage

import java.time.LocalDate

class TimestampBlocking(roleMap: Map[String, RoleLineage], ts: LocalDate) {

  def getBlockOfPair(l: Long) = {
    assert(l<nPairsInBlocking)
    blocks.maxBefore(l+1).get
  }

  def getBlocks() = {
    val blocksAsTreeMap = collection.mutable.TreeMap[Long,Block]()
    var curSum:Long = 0
    roleMap.groupBy{case (id,r) => r.valueAt(ts)}
      .withFilter{case (v,map) => !GLOBAL_CONFIG.nonInformativeValues.contains(v) && !RoleLineage.isWildcard(v) && map.size>1}
      .foreach{case (v,map) => {
        val b = new Block(v,map.keySet.toIndexedSeq)
        blocksAsTreeMap.put(curSum,b)
        curSum += b.nPairs
      }}
    blocksAsTreeMap
  }

  val blocks = getBlocks()

  val nPairsInBlocking = blocks.values.map(_.nPairs).sum

}
