package de.hpi.dataset_versioning.oneshot

import java.lang.AssertionError
import scala.collection.mutable

object EntropyShenanigansMain extends App {

  case class FieldLineage(lineage:String, label:String) {
    def printWithEntropy = println(toString + f"($defaultEntropy%1.3f)")

    def defaultEntropy = entropyV3

    def mergeCompatible(other: FieldLineage) = {
      if(lineage.size != other.lineage.size) throw new AssertionError("not same size")
      val s1 = lineage
      val s2 = other.lineage
      val newSequence = (0 until s1.size).map(i => {
        if(s1(i) == s2(i)) s1(i)
        else if(s1(i) =='_') s2(i)
        else if(s2(i) =='_') s1(i)
        else throw new AssertionError(s"not compatible at index ${i}")
      })
      FieldLineage(newSequence.mkString,label + "&" + other.label)
    }

//DEPRECATED:
//    def entropyV1:Double = {
//      entropyV1(getTransitions(lineage))
//    }
//
//    def entropyV1(transitions: mutable.TreeMap[(Char, Char), Int]):Double = {
//      - transitions.values.map(count => {
//        val pXI = count / transitions.values.sum.toDouble
//        pXI * log2(pXI)
//      }).sum
//    }

    def entropyV3:Double = {
      entropyV2(getTransitions(lineage,true),lineage.length)
    }

    def entropyV2:Double = {
      entropyV2(getTransitions(lineage),lineage.length)
    }

    def entropyV2(transitions: mutable.TreeMap[(Char, Char), Int], lineageSize:Int):Double = {
      - transitions.values.map(count => {
        val pXI = count / (lineageSize-1).toDouble
        pXI * log2(pXI)
      }).sum
    }

    def log2(a:Double) = math.log(a) / math.log(2)

    def getTransitions(stringWithWildcards: String,countOnlyTrueChange:Boolean=false) = {
      val stringWithoutWildcards = stringWithWildcards.filter(_ != '_')
      var prev = stringWithoutWildcards(0)
      val transitions = mutable.TreeMap[(Char,Char),Int]()
      stringWithoutWildcards.tail.foreach(c => {
        if(!countOnlyTrueChange || c!=prev) {
          val prevCount = transitions.getOrElseUpdate((prev, c), 0)
          transitions((prev, c)) = prevCount + 1
          prev = c
        }
      })
      transitions
    }

    override def toString: String = s"$label:[" + lineage.mkString(",") + "]"

  }

  //C_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
  //CCCCCCCCCCCCCCCCCCCCCCCCCCCCCC_B

//  val b = "__________BBBBBBBBB"
//  val toMergeWithB = IndexedSeq(
//    "BBBBBBBBBBBBBBBBBBB",
//    "_________________BB",
//    "________________BBB",
//    "_______________BBBB",
//    "______________BBBBB",
//    "AA____________BBBBB",
//    "ACAD__________BBBBB",
//    "AAAA__________BBBBB",
//    "AAAB__________BBBBB",
//    "AABB__________BBBBB",
//    "_________________BB",
//    "BB_________________",
//  )
//  mergeAllAndPrint(b,toMergeWithB)
  val c = "_____ABBBB"
  val toMergeWithC = IndexedSeq(
    "_____ABBBB",
    "ABBBB_____",
    "AB________",
    "_____AB___",
    "_____ABB__",
  )
  mergeAllAndPrint(c,toMergeWithC)

  val d = "ABCD____"
  val e = "A_B_C_D_"
  val f = "AABBCCDD"

  val g = "AA__BB__"
  val h = "AAAABBBB"
  val i = "AACCBBCC"

  Seq(d,e,f,g,h,i).zipWithIndex.map{case (s,i) => FieldLineage(s,s"#$i")}
    .foreach(_.printWithEntropy)

//  val d = "ABCD_____"
//  val e = "ABCD_D_D_"
//  val f = "AB__C_C_C"
//  val def_ = "ABCDCDCDC"
//  val de = "ABCD_D_D_"
//  println(entropyV2(d))
//  println(entropyV2(e))
//  println(entropyV2(f))
//  println(entropyV2(def_))
//  println(entropyV2(de))
//  val asdasf = getTransitions(f)

  def mergeAllAndPrint(elem:String, elems:Seq[String]) = {
    val results = elems.zipWithIndex.map{case (s,i) => MergeMatch(FieldLineage(elem,"#0"), FieldLineage(s,s"#${i+1}"))}
    results.sortBy(-_.entropyReduction)
      .foreach(_.printShort)
    println("---------------------------------------------------------------------------------------")
  }

}
