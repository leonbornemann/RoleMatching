package de.hpi.role_matching.evaluation.semantic

class Block(val key: Any, val roleIDSInBlock: IndexedSeq[String]) {

  def getPair(pairID: Long) = {
    if(!(pairID < nPairs)){
      println(pairID)
      println(nPairs)
    }
    assert(pairID< nPairs)
    if(pairID<roleIDSInBlock.size-1){
      (roleIDSInBlock(0), roleIDSInBlock(pairID.toInt+1))
    } else {
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
      val actualSecondIndex = curFirstMatchPartnerIndex + secondMatchPartnerIndex.toInt
      if(curFirstMatchPartnerIndex==actualSecondIndex){
        println("WHAAAAT?")
        println(curFirstMatchPartnerIndex,actualSecondIndex)
        println(roleIDSInBlock(curFirstMatchPartnerIndex),roleIDSInBlock(actualSecondIndex))
      }
      (roleIDSInBlock(curFirstMatchPartnerIndex),roleIDSInBlock(actualSecondIndex))
    }
  }


  def nPairs = gaussSum(roleIDSInBlock.size-1)
  def gaussSum(n: Int) = n.toLong*(n+1).toLong/2
}
