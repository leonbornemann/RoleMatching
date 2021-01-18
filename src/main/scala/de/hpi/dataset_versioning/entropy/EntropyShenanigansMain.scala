package de.hpi.dataset_versioning.entropy

import java.io.File
import scala.collection.mutable
import scala.io.Source

object EntropyShenanigansMain extends App {

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
  mergeAllAndPrint(c, toMergeWithC)

  val d = "ABCD____"
  val e = "A_B_C_D_"
  val f = "AABBCCDD"

  val g = "AA__BB__"
  val h = "AAAABBBB"
  val i = "AACCBBCC"


  val x = "ABBBB_____"
  val y = "__BBBABBBB"
  val z = "ABBBBABBBB"

  val sequences = Seq(d, e, f, g, h, i, x, y, z).zipWithIndex.map { case (s, i) => FieldLineage(s, s"#$i") }
  sequences.foreach(_.printWithEntropy)
  //sequences.last.mergeCompatible(sequences(sequences.size-2)).printWithEntropy
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

  def mergeAllAndPrint(elem: String, elems: Seq[String]) = {
    val results = elems.zipWithIndex.map { case (s, i) => MergeMatch(FieldLineage(elem, "#0"), FieldLineage(s, s"#${i + 1}")) }
    results.sortBy(-_.entropyReduction)
      .foreach(_.printShort)
    println("---------------------------------------------------------------------------------------")
  }

  val lineageMatches = loadFromFile("/home/leon/data/dataset_versioning/associationEdgeValues/edgeContentsAsStrings.csv")
  println(lineageMatches.groupBy(_.size).map(t => (t._1,t._2.size)))
  println(lineageMatches.filter(_.size==1))
  println(lineageMatches
    .filter(_.size==2)
    .map(fls => MergeMatch(fls(0),fls(1)))
    .map(a => (a.entropyDifferenceBeweenOriginals))
    .filter(_ >0)
    .size)


  def loadFromFile(file: String) = {
    val values = Source.fromFile(new File(file))
      .getLines()
      .toIndexedSeq
      .tail
      .map(l => {
        val values = l.split(",").toIndexedSeq
        //5ztz-espx.0_1,533,5ztz-espx.0_2,563,AAAAAAAAAAAAAAAAAAAAA
        val lineageStrings = values(4).split(";").toIndexedSeq
        lineageStrings.map(s => FieldLineage(s, values(0)))
      })
    values
  }

}
