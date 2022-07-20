package de.hpi.role_matching.ditto

import de.hpi.role_matching.evaluation.semantic.Block

class SimpleBlocking(inputBlocks: IndexedSeq[Block]) {

  def getBlockOfPair(l: Long) = {
    assert(l<nPairsInBlocking)
    blocks.maxBefore(l+1).get
  }

  def getBlocks() = {
    val blocksAsTreeMap = collection.mutable.TreeMap[Long,Block]()
    var curSum:Long = 0
    inputBlocks
      .withFilter{case (b) => b.nPairs>0}
      .foreach{case (b) => {
        blocksAsTreeMap.put(curSum,b)
        curSum += b.nPairs
      }}
    blocksAsTreeMap
  }

  val blocks = getBlocks()

  val nPairsInBlocking = blocks.values.map(_.nPairs).sum

}
