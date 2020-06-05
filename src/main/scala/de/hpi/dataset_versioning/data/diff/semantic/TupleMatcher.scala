package de.hpi.dataset_versioning.data.diff.semantic

import com.google.gson.JsonElement
import com.typesafe.scalalogging.StrictLogging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TupleMatcher() extends StrictLogging {

  val sizeThreshold = 100
  val matchThreshold = 0.5

  def calculateMatchingScore(tl: Set[(String, JsonElement)], tr: Set[(String, JsonElement)]) = {
    tl.intersect(tr).size.toDouble / tl.union(tr).size
  }

  def matchTuples(leftTuples:  Map[Set[(String, JsonElement)], ArrayBuffer[Set[(String, JsonElement)]]], rightTuples:  Map[Set[(String, JsonElement)], ArrayBuffer[Set[(String, JsonElement)]]]) = {
    var unmatchedLeft = mutable.ArrayBuffer[Set[(String, JsonElement)]]()
    var unmatchedRight = mutable.ArrayBuffer[Set[(String, JsonElement)]]()
    val updates = mutable.HashMap[Set[(String, JsonElement)],Set[(String, JsonElement)]]()
    val diff = new RelationalDatasetDiff(mutable.HashSet[Set[(String, JsonElement)]](),mutable.HashSet[Set[(String, JsonElement)]](),mutable.HashSet[Set[(String, JsonElement)]](),updates)
    leftTuples.keySet.union(rightTuples.keySet).foreach(t => {
      val left = leftTuples.getOrElse(t,Seq())
      val right = rightTuples.getOrElse(t,Seq())
      if(left.size==right.size){
        //perfect match nothing to add to diff - TODO: should we add an artificial row number column?#
        diff.unchanged ++= left
      } else if(left.size > right.size){
        val sizeDifference = left.size-right.size
        if(sizeDifference != left.size)
          diff.unchanged ++= left.slice(0,left.size - sizeDifference)
        unmatchedLeft ++= left.slice(0,sizeDifference)
      } else {
        val sizeDifference = right.size-left.size
        if(sizeDifference != right.size)
          diff.unchanged ++= right.slice(0,right.size - sizeDifference)
        unmatchedRight ++= right.slice(0,right.size-left.size)
      }
      ()
    })
    if (unmatchedLeft.size > sizeThreshold || unmatchedRight.size > sizeThreshold) {
      diff.inserts ++= unmatchedRight
      diff.deletes ++= unmatchedLeft
      diff.incomplete=true
      diff
    } else {
      val matchedRight = mutable.HashSet[Int]()
      val matchedLeft = mutable.HashSet[Int]()
      for (i <- 0 until unmatchedLeft.size) {
        val tl = unmatchedLeft(i)
        var curMatchScore = -1.0
        var curMatchIndex = -1
        for (j <- 0 until unmatchedRight.size) {
          val tr = unmatchedRight(j)
          val matchingScore = calculateMatchingScore(tl,tr)
          if(matchingScore >= matchThreshold && matchingScore > curMatchScore && !matchedRight.contains(j)){
            curMatchIndex = j
            curMatchScore = matchingScore
          }
        }
        if(curMatchIndex != -1){
          val match_ = unmatchedRight(curMatchIndex)
          updates.put(tl,match_)
          matchedRight.add(curMatchIndex)
          matchedLeft.add(i)
        }
      }
      for (i <- 0 until unmatchedLeft.size) {
        if(!matchedLeft.contains(i))
          diff.deletes.add(unmatchedLeft(i))
      }
      for (i <- 0 until unmatchedRight.size) {
        if(!matchedRight.contains(i))
          diff.inserts.add(unmatchedRight(i))
      }
    }
    diff
  }

}
