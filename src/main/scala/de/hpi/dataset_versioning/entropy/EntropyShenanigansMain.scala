package de.hpi.dataset_versioning.entropy

import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.optimization.TupleMerge
import de.hpi.dataset_versioning.io.IOService
import de.hpi.dataset_versioning.util.{MathUtil, TableFormatter}

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

  def printMutualInfo(str: String, str1: String) = {
    println(s"MI($str,$str1)")
    val mutualInfo = MergeMatch.mutualInformationWCNOTEQUALWC(FieldLineageAsCharacterString(str,"1"), FieldLineageAsCharacterString(str1,"2"))
    println(mutualInfo)
    println("---------------------------------------------------------------------------------")
  }

  val pairsWithoutWildcards = IndexedSeq(
    ("AAAAAAAAAA","AAAAAAAAAA"),
    ("AAAAAAAAAB","AAAAAAAAAB"),
    ("AAAAAAAABC","AAAAAAAABC"),
    ("AAAAAAABCD","AAAAAAABCD"),
    ("AAAAAABCDE","AAAAAABCDE"),
    ("ABCDEFGHIJ","ABCDEFGHIJ")
  //  ("ABABABABAB","ABABABABAB"),
  //  ("AAABBBCCCD","AAABBBCCCD")
  )

  val differentRepeatingDynamic = IndexedSeq(
    ("ABABABABAB","ABABABABAB"),
    ("AABBAABBAA","AABBAABBAA"),
    ("AAABBBAAAB","AAABBBAAAB"),
    ("AAAABBBBAA","AAAABBBBAA")
  )

  val graduallyMoreWildcardsRHS = IndexedSeq(
    ("AAABBBCCCD","AAABBBCCCD"),
    ("AAABBBCCCD","_AABBBCCCD"),
    ("AAABBBCCCD","__ABBBCCCD"),
    ("AAABBBCCCD","___BBBCCCD"),
    ("AAABBBCCCD","____BBCCCD"),
    ("AAABBBCCCD","_____BCCCD"),
    ("AAABBBCCCD","______CCCD"),
    ("AAABBBCCCD","_______CCD"),
    ("AAABBBCCCD","________CD"))

  val overlappingWildcards = IndexedSeq(
    ("AAABBBCCCD","AAABBBCCCD"),
    ("_AABBBCCCD","_AABBBCCCD"),
    ("__ABBBCCCD","__ABBBCCCD"),
    ("___BBBCCCD","___BBBCCCD"),
    ("____BBCCCD","____BBCCCD"),
    ("_____BCCCD","_____BCCCD"),
    ("______CCCD","______CCCD"),
    ("_______CCD","_______CCD"),
    ("________CD","________CD")
  )

  val graduallyMoreWildcardsBothSides = IndexedSeq(
    ("AAABBBCCC_","_AABBBCCCD"),
    ("AAABBBCC__","__ABBBCCCD"),
    ("AAABBBC___","___BBBCCCD"),
    ("AAABBB____","____BBCCCD")
  )

  val lessTransitions = IndexedSeq(
    ("AAAAABBBBB","AAAAABBBBB"),
    ("__AAABBBBB","AAAAABBB__"),
    ("____ABBBBB","AAAAAB____"),
    ("____ABBBBB","____AB____"))

  def printMEtricTableFromMergeMAtches(mergeMatches: IndexedSeq[MergeMatch],useFLIdentifiers: Boolean = false) = {
    val header = Seq("#","FL1","FL2","FL_Merged","MI (WC!=WC)","MI (WC=WC)","E-Reduction")
    val rows = mergeMatches.zipWithIndex.map{case (mergeMatch,i) => {
      getRowString(i, mergeMatch,useFLIdentifiers)
    }}
    startPairNumber += mergeMatches.size
    val rowsWithHEader:Seq[Seq[Any]] = Seq(header) ++ rows
    val res = TableFormatter.format(rowsWithHEader)
    println(res)
  }

  def printMEtricTable(pairs: IndexedSeq[(String, String)]) = {
    val mergeMatches = pairs.zipWithIndex.map{case (t,i) => {
      val a = MergeMatch(FieldLineageAsCharacterString(t._1,"1"),FieldLineageAsCharacterString(t._2,"2"))
      a
    }}
    printMEtricTableFromMergeMAtches(mergeMatches)
  }

  def getRowString(i: Int, mergeMatch: MergeMatch,useFLIdentifiers:Boolean=false) = {
    val id = if(useFLIdentifiers) s"${mergeMatch.first.label} ⊔ ${mergeMatch.second.label}" else i+startPairNumber
    f"$id, ${mergeMatch.first.lineage} , ${mergeMatch.second.lineage} , ${mergeMatch.merged.lineage} ,${mergeMatch.mutualInformationWCNOTEQUALWC}%1.3f,${mergeMatch.mutualInformationWCEQUALTOWC}%1.3f,${mergeMatch.entropyReduction}%1.3f".split(",").toIndexedSeq
  }

  var startPairNumber = 0
  println("No Wildcards")
  printMEtricTable(pairsWithoutWildcards)
  println("No Wildcards - differentRepeatingDynamic")
  printMEtricTable(differentRepeatingDynamic)
  println("One-Sided Wildcards")
  printMEtricTable(graduallyMoreWildcardsRHS)
  println("Double-Sided Overlapping Wildcards")
  printMEtricTable(overlappingWildcards)
  println("Double-Sided Non-Overlapping Wildcards")
  printMEtricTable(graduallyMoreWildcardsBothSides)
  println("Less Different Transitions")
  printMEtricTable(lessTransitions)


  def testSingleMutualInfo() = {
    printMutualInfo("ABCDEFGHIJ","__________")
    printMutualInfo("ABCDEFGHIJ","0123456789")
    printMutualInfo("ABCDEFGHIJ","ABCDEFGHIJ")
    printMutualInfo("AAAAAAAAAA","AAAAAAAAAA")
    printMutualInfo("AAAAAAAAAA","BBBBBBBBBB")
    printMutualInfo("AAAAAAAAAA","0123456789")
    printMutualInfo("AAAAAAAAAB","0123456789")
  }

  val matchingsToRank = IndexedSeq(
    ("___BBBCCCD","BBBBBBCCCD"),
    ("___BBBCCCD","___BBBCCCD"),
    ("___BBBCCCD","CCCBBBCCCD"),
    ("___BBBCCCD","CBCBBBCCCD"),
    ("___BBBCCCD","AEABBBCCCD"),
    ("___BBBCCCD","AAABBBC___"),
    ("___BBBCCCD","___BBBC___"),
    ("___BBBCCCD","_____BC___"),
    ("___BBBCCCD","AAA_____CD"),
    ("___BBBCCCD","BBB_____CD"),
    ("___BBBCCCD","CCC_____CD"),
    ("___BBBCCCD","________CD")
  )

  val checkIfBadResult = IndexedSeq(
    ("AAAAAAAAA_________ABCDEFGHI","_________AAAAAAAAAABCDEFGHI"),
    ("AAAAAAAAA_________ABCDEFGHI","AAAAAAAAAB________AB_______"),
    ("AAAAAAAAA_________ABCDEFGHI","_________BAAAAAAAAAB_______"),
  )

  val mergeMatches = matchingsToRank.zipWithIndex.map{case ((fl1,fl2),i) => {
    MergeMatch(FieldLineageAsCharacterString(fl1,s"#0"),FieldLineageAsCharacterString(fl2,s"#$i"))
  }}
  startPairNumber = 0
  val byEntropyReduction = mergeMatches.sortBy(-_.entropyReduction)
  println("byEntropyReduction")
  printMEtricTableFromMergeMAtches(byEntropyReduction,true)
  startPairNumber = 0
  val byMutualInfo = mergeMatches.sortBy(-_.mutualInformationWCNOTEQUALWC)
  println("byMutualInfo")
  printMEtricTableFromMergeMAtches(byMutualInfo,true)
  println()

  println("New Experiment")
  printMEtricTable(checkIfBadResult)
  println()

  val newLineages = IndexedSeq(
    "a_aba__a__a_",
    "a_abab_ab_a_",
    "a_aba_ca_ca_",
    "_dab_______d"
  )
  IOService.STANDARD_TIME_FRAME_END = IOService.STANDARD_TIME_FRAME_START.plusDays(newLineages(0).length-1)
  val maxClique = newLineages.zip('A' until ('A' + newLineages.size).toChar).map(t => FieldLineageAsCharacterString(t._1,t._2.toChar.toString))
  maxClique.foreach(println)
  val powerset = MathUtil
    .powerset(maxClique.toList)
    .filter(!_.isEmpty)
  val withAverageEvidence = powerset.map(clique =>
    (clique,ValueLineageClique(clique.toIndexedSeq.map(_.toValueLineage)).averageEvidenceToMergedResultScore))
  val withAverageMI = powerset.map(clique =>
    (clique,ValueLineageClique(clique.toIndexedSeq.map(_.toValueLineage)).averageMutualInformation))
  val withEntropyReduction = powerset.map(clique =>
    (clique,ValueLineageClique(clique.toIndexedSeq.map(_.toValueLineage)).entropyReduction))

  printCliqueMergeTable(withAverageEvidence,"averageEvidence")
  println()
  printCliqueMergeTable(withEntropyReduction,"entropyReduction")
  println()
  printCliqueMergeTable(withAverageMI,"averageMutualInfo")

  def printCliqueMergeTable(withScores: List[(List[FieldLineageAsCharacterString], Double)],scoreName:String) = {
    val header = Seq("#","Clique","Merge-Result",s"$scoreName")
    val rows = withScores
      .sortBy(-_._2)
      .zipWithIndex
      .map{case (mergeMatch,i) => {
        Seq(s"$i", mergeMatch._1.map(_.label).sorted.mkString,s"${FieldLineageAsCharacterString.mergeAll(mergeMatch._1)}", f"${mergeMatch._2}%.5f")
      }}
    val rowsWithHEader:Seq[Seq[Any]] = Seq(header) ++ rows
    val res = TableFormatter.format(rowsWithHEader)
    println(res)
  }
//  testSingleMutualInfo()
//  val c  = "_____ABBBB"
//  val c1 = "_____ABBBB"
//  val res = MergeMatch(FieldLineageAsCharacterString(c,"1"), FieldLineageAsCharacterString(c1,"2")).mutualInformationWCNOTEQUALWC
//  val toMergeWithC = IndexedSeq(
//    "_____ABBBB",
//    "ABBBB_____",
//    "AB________",
//    "_____AB___",
//    "_____ABB__"
//  )
//  mergeAllAndPrint(c, toMergeWithC)
//
//  val d = "_____ABBBB_"
//  val toMergeWithD = IndexedSeq(
//    "_____ABB__Z",
//    "_____ABBBBB"
//  )
//  mergeAllAndPrint(d,toMergeWithD)
//  println()
//  analyzeRealMatches

  //  val d = "ABCD____"
//  val e = "A_B_C_D_"
//  val f = "AABBCCDD"
//
//  val g = "AA__BB__"
//  val h = "AAAABBBB"
//  val i = "AACCBBCC"
//
//
//  val x = "ABBBB_____"
//  val y = "__BBBABBBB"
//  val z = "ABBBBABBBB"
//
//  val sequences = Seq(d, e, f, g, h, i, x, y, z).zipWithIndex.map { case (s, i) => FieldLineageAsCharacterString(s, s"#$i") }
//  sequences.foreach(_.printWithEntropy)
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
    results.sortBy(-_.scoreFunction)
      .foreach(_.printShort)
    println("---------------------------------------------------------------------------------------")
  }

  private def analyzeRealMatches = {
    val WORKING_DIR = args(1)
    val lineageMatches = loadFromFile(WORKING_DIR + "edgeContentsAsStrings.csv")
    println(lineageMatches.groupBy(_.size).map(t => (t._1, t._2.size)))
    println(lineageMatches.filter(_.size == 1))
    val mergeMatches: IndexedSeq[MergeMatch] = lineageMatches
      .filter(_.size == 2)
      .map(fls => MergeMatch(fls(0), fls(1)))
    println(mergeMatches.size)
    println(mergeMatches.filter(_.entropyReduction > 0).size)
    val nonEqualMatches = mergeMatches.filter(m => m.first.lineage != m.second.lineage)
    println(nonEqualMatches.size)
    val byDifference = nonEqualMatches.sortBy(m => -Seq(m.first.lineage.toSet.size, m.second.lineage.toSet.size).max)
    val biggestAlphabetDifference: MergeMatch = byDifference.head
    biggestAlphabetDifference.printShort
    biggestAlphabetDifference.printActualTuples
    biggestAlphabetDifference.exportActualTableMatch(s"$WORKING_DIR/${biggestAlphabetDifference.first.label}_MERGE_${biggestAlphabetDifference.second.label}.txt")
    biggestAlphabetDifference.printShort
    println(mergeMatches
      .map(a => (a.entropyReduction))
      .filter(_ > 0)
      .size)
  }


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
