package de.hpi.role_matching.evaluation.semantic

import de.hpi.role_matching.cbrm.data.Util

class Block(val key: Any, val roleIDSInBlock: IndexedSeq[String]) {

  def getPair(pairID: Long) = {
    if(!(pairID < nPairs)){
      println(pairID)
      println(nPairs)
    }
    assert(pairID< nPairs)
    //find first match partner first:
    var curFirstMatchPartnerIndex = 0
    var curMatchPartnerCount = roleIDSInBlock.size-1
    var secondMatchPartnerIndex = pairID
    while(secondMatchPartnerIndex>curMatchPartnerCount){
      assert(curMatchPartnerCount>0)
      secondMatchPartnerIndex -= curMatchPartnerCount
      curMatchPartnerCount -= 1
      curFirstMatchPartnerIndex +=1
    }
    assert(secondMatchPartnerIndex<roleIDSInBlock.size)
    (roleIDSInBlock(curFirstMatchPartnerIndex),roleIDSInBlock(secondMatchPartnerIndex.toInt))
  }


  def nPairs = gaussSum(roleIDSInBlock.size-1)
  def gaussSum(n: Int) = n.toLong*(n+1).toLong/2
}
