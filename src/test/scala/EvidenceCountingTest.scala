import de.hpi.socrata.change.{ReservedChangeValues, UpdateChangeCounter}
import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage
import de.hpi.role_matching.GLOBAL_CONFIG

import java.time.LocalDate
import scala.collection.mutable
import scala.util.Random

object EvidenceCountingTest extends App {

  val counter = new UpdateChangeCounter()

  def fromSeq(str: String) = {
    val values = str.map(c => if(c=='_') rWC() else c.toString)
    val res = mutable.TreeMap[LocalDate,Any]() ++  (values.zipWithIndex
      .map{case (v,i) => {
        if(i==0 || values(i-1) !=v )
          Some((LocalDate.of(2019,11,1).plusDays(i),v))
        else
          None
      }})
      .filter(_.isDefined)
      .map(_.get)
    FactLineage(res)
  }

  //choose wildcards randomly:
  val wildcards = Seq(ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_COL,ReservedChangeValues.NOT_EXISTANT_DATASET)
  val random = new Random(42)

  def rWC() = {
    val i = random.nextInt(wildcards.size)
    wildcards(i)
  }
  var valueLineage = fromSeq("a")
  //for Interleaved:
  GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS = true
  for(_ <- 0 until 100){
    //to self:
    valueLineage = fromSeq("a")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==0)
    valueLineage = fromSeq("ab")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==1)
    valueLineage = fromSeq("a_b")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==1)
    valueLineage = fromSeq("_____aaaaa____a_____a")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==0)
    valueLineage = fromSeq("_____aaaaa____b____a___")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==2)
    valueLineage = fromSeq("aa_____aaaaa____b____a___")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==2)
    valueLineage = fromSeq("b____aaaaa____b____a___")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==3)
    valueLineage = fromSeq("b____aaaaa____b____ac___")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==4)
    //pairs:
    var valueLineage1 = fromSeq("_____aaa__cccca_____a")
    var valueLineage2 = fromSeq("_____aaaaacccca_____a")
    assert(valueLineage1.getOverlapEvidenceCount(valueLineage2)==2)
    valueLineage1 = fromSeq("a__________ccccccccaaaaaaa")
    valueLineage2 = fromSeq("a______________cccca_____a")
    assert(valueLineage1.getOverlapEvidenceCount(valueLineage2)==1)
    valueLineage1 = fromSeq("ab_________ccccccb_______a")
    valueLineage2 = fromSeq("a__________cccc____b_____a")
    assert(valueLineage1.getOverlapEvidenceCount(valueLineage2)==1)
  }
  GLOBAL_CONFIG.ALLOW_INTERLEAVED_WILDCARDS_BETWEEN_EVIDENCE_TRANSITIONS = false
  for(_ <- 0 until 100){
    //to self:
    valueLineage = fromSeq("a")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==0)
    valueLineage = fromSeq("ab")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==1)
    valueLineage = fromSeq("a_b")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==0)
    valueLineage = fromSeq("_____aaaaa____a_____a")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==0)
    valueLineage = fromSeq("_____aaaaa____b____a___")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==0)
    valueLineage = fromSeq("aa_____aaaaa____b____a___")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==0)
    valueLineage = fromSeq("b____aaaaa____b____a___")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==0)
    valueLineage = fromSeq("b____aaaaa____b____ac___")
    assert(valueLineage.getOverlapEvidenceCount(valueLineage)==1)
    var valueLineage1 = fromSeq("_____aaa__cccca_____a")
    var valueLineage2 = fromSeq("_____aaaaacccca_____a")
    assert(valueLineage1.getOverlapEvidenceCount(valueLineage2)==1)
    valueLineage1 = fromSeq("a__________ccccccccaaaaaaa")
    valueLineage2 = fromSeq("a______________cccca_____a")
    assert(valueLineage1.getOverlapEvidenceCount(valueLineage2)==1)
    valueLineage1 = fromSeq("ab_________ccccccb_______a")
    valueLineage2 = fromSeq("a__________cccc____b_____a")
    assert(valueLineage1.getOverlapEvidenceCount(valueLineage2)==0)
    //first pair:
    //val vl1 = fromSeq()
  }

  //TODO:
//  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
//  val wildcards = Seq(ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_COL,ReservedChangeValues.NOT_EXISTANT_DATASET)
//  for(wc <- wildcards) {
//    valueLineage = fromSeq(Seq(wc,"a"))
//    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
//    valueLineage = fromSeq(Seq(wc,"a",wc))
//    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
//    valueLineage = fromSeq(Seq(wc,"a",wc,"a"))
//    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
//    valueLineage = fromSeq(Seq("a",wc,wc,wc,"a",wc,"a",wc,wc,"b",wc,"b","b"))
//    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
//    valueLineage = fromSeq(Seq("a","a","a",wc,"b",wc,"b","b"))
//    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
//  }
//  valueLineage = fromSeq(wildcards ++ Seq("a") ++ wildcards.flatMap(wc => Seq(wc,"a")))
//  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
//
//  valueLineage = fromSeq(Seq("a","b"))
//  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
//  valueLineage = fromSeq(Seq(ReservedChangeValues.NOT_EXISTANT_DATASET,ReservedChangeValues.NOT_EXISTANT_ROW,"a",ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_DATASET,"a",
//    ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_DATASET,"b",ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_DATASET,"b"))
//  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
//  valueLineage = fromSeq(Seq("a","b","a","b","a","b","a","b"))
//  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==7)
//  valueLineage = fromSeq(Seq("a","a","a","b","b","b"))
//  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
}
