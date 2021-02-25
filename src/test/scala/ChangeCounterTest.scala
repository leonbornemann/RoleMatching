import java.time.LocalDate
import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.UpdateChangeCounter
import de.hpi.dataset_versioning.db_synthesis.sketches.field.AbstractTemporalField
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object ChangeCounterTest extends App {

  val counter = new UpdateChangeCounter

  def fromSeq(value: Seq[String]) = {
    val res = mutable.TreeMap[LocalDate,Any]() ++  (value.zipWithIndex
      .map{case (v,i) => {
        if(i==0 || value(i-1) !=v )
          Some((LocalDate.of(2019,11,1).plusDays(i),v))
        else
          None
      }})
      .filter(_.isDefined)
      .map(_.get)
    res
  }

  var valueLineage = fromSeq(Seq("a"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
  val wildcards = Seq(ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_COL,ReservedChangeValues.NOT_EXISTANT_DATASET)
  for(wc <- wildcards) {
    valueLineage = fromSeq(Seq(wc,"a"))
    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
    valueLineage = fromSeq(Seq(wc,"a",wc))
    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
    valueLineage = fromSeq(Seq(wc,"a",wc,"a"))
    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)
    valueLineage = fromSeq(Seq("a",wc,wc,wc,"a",wc,"a",wc,wc,"b",wc,"b","b"))
    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
    valueLineage = fromSeq(Seq("a","a","a",wc,"b",wc,"b","b"))
    assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
  }
  valueLineage = fromSeq(wildcards ++ Seq("a") ++ wildcards.flatMap(wc => Seq(wc,"a")))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==0)

  valueLineage = fromSeq(Seq("a","b"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
  valueLineage = fromSeq(Seq(ReservedChangeValues.NOT_EXISTANT_DATASET,ReservedChangeValues.NOT_EXISTANT_ROW,"a",ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_DATASET,"a",
    ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_DATASET,"b",ReservedChangeValues.NOT_EXISTANT_ROW,ReservedChangeValues.NOT_EXISTANT_DATASET,"b"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
  valueLineage = fromSeq(Seq("a","b","a","b","a","b","a","b"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==7)
  valueLineage = fromSeq(Seq("a","a","a","b","b","b"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)._1==1)
}
