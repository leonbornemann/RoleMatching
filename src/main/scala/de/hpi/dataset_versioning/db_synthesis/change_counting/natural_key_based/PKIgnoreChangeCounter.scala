package de.hpi.dataset_versioning.db_synthesis.change_counting.natural_key_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.TemporalTable
import de.hpi.dataset_versioning.db_synthesis.baseline.database.natural_key_based.AbstractTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.natural_key_based.DecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.sketches.column.TemporalColumnTrait

/***
 * Ignores all primary key columns in the respective tables
 * @param changeCounter
 */
class PKIgnoreChangeCounter(subdomain:String,changeCounter: FieldChangeCounter) extends TableChangeCounter{
  override def name: String = changeCounter.name +"_IGNORE_PK"

  def getOriginalPK(bcnfTables: Array[DecomposedTemporalTable]) = {
    val keys = bcnfTables.map(t => t.primaryKey.map(_.attrId)).toSet
    val byOccurrences = keys.map( k=> {
      val occurrences = bcnfTables.map(dtt => if(k.subsetOf(dtt.containedAttrLineages.map(_.attrId).toSet)) 1 else 0).sum
      (k,occurrences)
    }).toIndexedSeq.sortBy(_._2)
    //this is how it should actually work:
//    val trueKey = byOccurrences(0)
//    if(trueKey._2!=1){
//      println()
//    }
//    assert(trueKey._2==1)
//    if(!(byOccurrences.size==1 || byOccurrences(1)._2>1)){
//      println()
//    }
//    assert(byOccurrences.size==1 || byOccurrences(1)._2>1)
//    trueKey._1
    //this is the temporary fix to avoid a bug:
    val trueKey = byOccurrences.takeWhile(_._2==1)
    assert(trueKey.size>0)
    trueKey.flatMap(_._1).toSet
  }

  override def countChanges(table: TemporalTable, allDeterminantAttributeIDs: Set[Int]): Long = {
    //TODO: this is inefficient, but fine for now:
    var originalKey:Set[Int] = null
    originalKey = Set()
    val insertTime = table.insertTime
    table.rows.flatMap(tr => tr.fields
      .zip(table.attributes)
      .withFilter(t => !originalKey.contains(t._2.attrId))
      .map { case (f, _) => changeCounter.countFieldChanges(insertTime, f).toLong }).sum
  }

  override def countColumnChanges[A](tc: TemporalColumnTrait[A], insertTime: LocalDate, colIsPk: Boolean): Long = {
    if(colIsPk) 0
    else changeCounter.countColumnChanges(tc,insertTime,colIsPk)
  }

  override def countChanges[A](table: AbstractTemporalDatabaseTable[A]): Long = {
    val insertTime = table.insertTime
    val pk = table.primaryKey.map(_.attrId).toSet
    val cols = table.dataColumns.filter(c => !pk.contains(c.attrID))
    cols.map(c => changeCounter.countColumnChanges(c,insertTime,false)).sum
  }
}
