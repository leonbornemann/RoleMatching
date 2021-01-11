package de.hpi.dataset_versioning.oneshot

import java.lang.AssertionError
import scala.collection.mutable

object EnthropyShenanigansMain extends App {

  val a = "____AABBC_B_B_C_DDDD"
  val toMergeWithA = IndexedSeq(
    "____AABBC_B_B_C_DDDD",
    //C_BEEEEE --> Count(C->B) + P(NO CHANGE || CHANGE TO B) * 1   [maybe OBSERVE B]
    //    --> Count(C->C) + P(NO CHANGE)
    //    --> Count(B->B) + P(CHANGE TO B)
    //X where X is in the lineage
    //    --> Count(C->X) + P(CHANGE && !CHANGE TO B)
    //    --> Count(X->B) + P(CHANGE && !CHANGE TO B)
    //contitional probability of (C->C AND C->B) vs (C->B AND B->B)
    // AEC___B
    "AAAAAABBC_B_B_C_DDDD",
    "EEEEAABBC_B_B_C_DDDD",
    "____AABBCCBCBCCCDDDD",
  )

  //C_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
  //CCCCCCCCCCCCCCCCCCCCCCCCCCCCCC_B

  mergeAllAndPrint(a,toMergeWithA)

  println("---------------------------------------------------------------------------------------")
  val b = "__________BBBBBBBBB"
  val toMergeWithB = IndexedSeq(
    "BBBBBBBBBBBBBBBBBBB"
  )
  mergeAllAndPrint(b,toMergeWithB)


  def mergeAllAndPrint(elem:String,elems:Seq[String]) = {
    elems.foreach(s => {
      println(s"$s:  ${entropyDifferenceAfterMerge(elem,s)}")
    })
  }

  def mergeCompatible(s1: String, s2: String) = {
    if(s1.size != s2.size) throw new AssertionError("not same size")
    val newSequence = (0 until s1.size).map(i => {
      if(s1(i) == s2(i)) s1(i)
      else if(s1(i) =='_') s2(i)
      else if(s2(i) =='_') s1(i)
      else throw new AssertionError(s"not compatible at index ${i}")
    })
    newSequence.mkString
  }

  def entropyDifferenceAfterMerge(s1:String, s2:String) = {
    math.abs(entropy(getTransitions(s1)) -entropy(getTransitions(mergeCompatible(s1,s2))))
  }

  def entropy(transitions: mutable.TreeMap[(Char, Char), Int]) = {
    - transitions.values.map(count => {
      val pXI = count / transitions.size.toDouble
      pXI * log2(pXI)
    }).sum
  }

  def log2(a:Double) = math.log(a) / math.log(2)

  def getTransitions(string: String) = {
    var prev = string(0)
    val transitions = mutable.TreeMap[(Char,Char),Int]()
    string.tail.foreach(c => {
      if(c!='_' && prev != '_'){
        val prevCount = transitions.getOrElseUpdate((prev,c),0)
        transitions((prev,c)) = prevCount+1
      }
      prev = c
    })
    transitions
  }

}
