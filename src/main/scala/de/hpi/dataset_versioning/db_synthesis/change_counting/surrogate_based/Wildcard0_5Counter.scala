package de.hpi.dataset_versioning.db_synthesis.change_counting.surrogate_based

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.data.change.temporal_tables.tuple.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch, SurrogateBasedTemporalRow}
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
import de.hpi.dataset_versioning.io.IOService

import java.time.LocalDate
import scala.collection.mutable

class Wildcard0_5Counter extends FieldChangeCounter{

  def countChangesForValueLineage[A](vl: mutable.TreeMap[LocalDate, A],isWildcard : (A => Boolean)):Float = {
    val it = vl.iterator
    var prev:Option[A] = None
    var cur:Option[A] = None
    var curChangeCount:Float = 0.0f
    while(it.hasNext){
      val (key,newElem) = it.next()
      //update pointers
      prev = cur
      cur = Some(newElem)
      if(prev==None){
        assert(key == IOService.STANDARD_TIME_FRAME_START)
      } else if(isWildcard(prev.get) && isWildcard(cur.get) || prev.get==cur.get){
        //nothing to count
      }
      else if(isWildcard(prev.get) || isWildcard(cur.get) )
        curChangeCount += 0.5f
      else
        curChangeCount += 1.0f
    }
    curChangeCount
  }


  def countFieldChanges(r: SurrogateBasedTemporalRow) = {
    val vl = r.value.getValueLineage
    countChangesForValueLineage[Any](vl,ValueLineage.isWildcard)
  }

  def countFieldChangesSimple[A](tuple: collection.Seq[TemporalFieldTrait[A]]) = {
    assert(tuple.size==1)
    val vl = tuple(0).getValueLineage
    countChangesForValueLineage[A](vl,tuple(0).isWildcard)
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociation) = {
    table.surrogateBasedTemporalRows.map(r => countFieldChanges(r)).sum
  }

  def countChanges(table:SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch) = {
    table.surrogateBasedTemporalRowSketches.map(r => countFieldChangesSimple(Seq(r.valueSketch))).sum
  }

  override def countChanges(table:TemporalTable) = {
    table.rows.flatMap(_.fields.map(vl => countChangesForValueLineage(vl.lineage,ValueLineage.isWildcard).toLong)).sum
  }

  override def countFieldChanges[A](viewInsertTime: LocalDate, f: TemporalFieldTrait[A]): Float = countFieldChangesSimple(Seq(f))

  override def name: String = "Wildcard0_5Counter"

  override def countChanges(table: TemporalTable, allDeterminantAttributeIDs: Set[Int]): Float = countChanges(table)

  override def countColumnChanges[A](tc: TemporalColumnTrait[A], insertTime: LocalDate, colIsPk: Boolean): Float = tc.fieldLineages
    .map(_.countChanges(insertTime,this)).sum
}
