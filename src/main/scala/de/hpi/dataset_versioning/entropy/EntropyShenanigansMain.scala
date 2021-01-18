package de.hpi.dataset_versioning.entropy

import de.hpi.dataset_versioning.io.IOService

import java.io.File
import scala.collection.mutable
import scala.io.Source

object EntropyShenanigansMain extends App {

  IOService.socrataDir = args(0)
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

  val sequences = Seq(d, e, f, g, h, i, x, y, z).zipWithIndex.map { case (s, i) => FieldLineageAsCharacterString(s, s"#$i") }
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
    val results = elems.zipWithIndex.map { case (s, i) => MergeMatch(FieldLineageAsCharacterString(elem, "#0"), FieldLineageAsCharacterString(s, s"#${i + 1}")) }
    results.sortBy(-_.entropyReduction)
      .foreach(_.printShort)
    println("---------------------------------------------------------------------------------------")
  }

  private val WORKING_DIR = args(1)
  val lineageMatches = loadFromFile(WORKING_DIR + "edgeContentsAsStrings.csv")
  println(lineageMatches.groupBy(_.size).map(t => (t._1,t._2.size)))
  println(lineageMatches.filter(_.size==1))
  private val mergeMatches: IndexedSeq[MergeMatch] = lineageMatches
    .filter(_.size == 2)
    .map(fls => MergeMatch(fls(0), fls(1)))
  val nonEqualMatches = mergeMatches.filter(m => m.first.lineage != m.second.lineage)
  val byDifference = nonEqualMatches.sortBy(m => -Seq(m.first.lineage.toSet.size,m.second.lineage.toSet.size).max)
  byDifference.head.exportActualTableMatch(s"$WORKING_DIR/${byDifference.head.first.label}_MERGE_${byDifference.head.second.label}.txt")
  byDifference.head.printShort
  println(mergeMatches
    .map(a => (a.entropyReduction))
    .filter(_ >0)
    .size)


  def loadFromFile(file: String) = {
    val values = Source.fromFile(new File(file))
      .getLines()
      .toIndexedSeq
      .tail
      .map(l => {
        val values = l.split(",").toIndexedSeq
        val lineageStringTokens = values(4).split(";")
        //5ztz-espx.0_1,533,5ztz-espx.0_2,563,AAAAAAAAAAAAAAAAAAAAA
        val rowNumbers1 = values(1).split(";")
        val rowNumbers2 = values(3).split(";")
        assert(lineageStringTokens.size == rowNumbers1.size + rowNumbers2.size)
        val lineages1 = rowNumbers1
          .zipWithIndex
          .map{case (rowNUm,i) =>
            FieldLineageAsCharacterString(lineageStringTokens(i),values(0),rowNUm.toInt)
          }
        val lineages2 = rowNumbers2
          .zipWithIndex
          .map{case (rowNUm,i) =>
            FieldLineageAsCharacterString(lineageStringTokens(i + rowNumbers1.size),values(2),rowNUm.toInt)
          }
        lineages1 ++ lineages2
//        val lineageStrings = lineageStringTokens.toIndexedSeq
//          .map(s => if(s.size==lineageStringTokens(0).size+1) s.substring(0,s.size-1) else s)
//        lineageStrings.map(s => FieldLineage(s, values(0)))
      })
    values
  }

}
