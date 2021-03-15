import EvidenceCountingTest.fromSeq
import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.field.MutualInformationComputer
import de.hpi.dataset_versioning.entropy.{FieldLineageAsCharacterString, MergeMatch}
import de.hpi.dataset_versioning.io.IOService

import java.time.LocalDate
import scala.collection.mutable
import scala.util.Random

object MutualInformationTest extends App {

  def fromSeq(str: String) = {
    val values = str.map(c => if(c=='_') rWC() else c.toString)
    val res = mutable.TreeMap[LocalDate,Any]() ++  (values.zipWithIndex
      .map{case (v,i) => {
        if(i==0 || values(i-1) !=v )
          Some(IOService.STANDARD_TIME_FRAME_START.plusDays(i),v)
        else
          None
      }})
      .filter(_.isDefined)
      .map(_.get)
    ValueLineage(res)
  }

  //choose wildcards randomly:
  val wildcards = Seq(ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_COL,ReservedChangeValues.NOT_EXISTANT_DATASET)
  val random = new Random(42)

  def rWC() = {
    val i = random.nextInt(wildcards.size)
    wildcards(i)
  }

  def compareBothImplementations(lineageString1: String, lineageString2: String) = {
    var valueLineage1 = fromSeq(lineageString1)
    var valueLineage2 = fromSeq(lineageString2)
    var vlAsCharString1 = FieldLineageAsCharacterString(lineageString1,"#1")
    var vlAsCharString2 = FieldLineageAsCharacterString(lineageString2,"#2")
    val resComplex = new MutualInformationComputer(vlAsCharString1.toValueLineage,vlAsCharString2.toValueLineage,lineageString1.length,IOService.STANDARD_TIME_FRAME_START.plusDays(lineageString1.length-1)).mutualInfo()
    val resSimple = MergeMatch(vlAsCharString1,vlAsCharString2).mutualInformationWCNOTEQUALWC
    if((resComplex-resSimple).abs>=0.00000001){
      println()
    }
    assert((resComplex-resSimple).abs<0.00000001)
  }

  val toCompare = IndexedSeq(
//    ("cccccccccb_","ccccccc____"),
//    ("ab_________ccccccb_______a","a__________cccc____b_____a"),
//    ("a__________ccccccccaaaaaaa","a______________cccca_____a"),
    ("_____aaa__cccca_____a","_____aaaaacccca_____a"),
    ("abaaaaaaccccccd","abaaaaaaccccccd"),
    ("abaaaaaacccccc","abaaaaaacccccc"),
    ("abaaaaaaccccc__","abaaaaaaccccccd"),
    ("__aaaaaaccccc__","abaaaaaaccccccd"),
    ("__aaaaaa_cccc__","abaaaaaaccccccd"),
  )

  toCompare.foreach(t => compareBothImplementations(t._1,t._2))
}
