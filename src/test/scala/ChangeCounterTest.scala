import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.ReservedChangeValues
import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based.UpdateChangeCounter
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object ChangeCounterTest extends App {

  val counter = new UpdateChangeCounter

  def fromSeq(value: Seq[String]) = {
    val res = mutable.TreeMap[LocalDate,Any]() ++  (value.zipWithIndex
      .map{case (v,i) => (LocalDate.of(2019,11,1).plusDays(i),v)})
    res
  }

  var valueLineage = fromSeq(Seq("a"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)==0)
  valueLineage = fromSeq(Seq(ReservedChangeValues.NOT_EXISTANT_COL,"a"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)==0)
  valueLineage = fromSeq(Seq(ReservedChangeValues.NOT_EXISTANT_DATASET,"a"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)==0)
  valueLineage = fromSeq(Seq(ReservedChangeValues.NOT_EXISTANT_DATASET,"a",ReservedChangeValues.NOT_EXISTANT_DATASET))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)==0)
  valueLineage = fromSeq(Seq(ReservedChangeValues.NOT_EXISTANT_DATASET,"a",ReservedChangeValues.NOT_EXISTANT_DATASET,"a"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)==0)
  valueLineage = fromSeq(Seq("a","b"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)==1)
  valueLineage = fromSeq(Seq(ReservedChangeValues.NOT_EXISTANT_DATASET,"a",ReservedChangeValues.NOT_EXISTANT_DATASET,"a",
    ReservedChangeValues.NOT_EXISTANT_DATASET,"b",ReservedChangeValues.NOT_EXISTANT_DATASET,"b"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)==1)
  valueLineage = fromSeq(Seq("a","b","a","b","a","b","a","b"))
  assert(counter.countChangesForValueLineage(valueLineage,ValueLineage.isWildcard)==7)
}
