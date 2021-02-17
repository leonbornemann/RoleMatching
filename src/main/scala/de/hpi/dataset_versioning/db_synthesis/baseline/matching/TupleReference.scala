package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.matching

@SerialVersionUID(3L)
case class TupleReference[A](table:TemporalDatabaseTableTrait[A], rowIndex:Int) extends Comparable[TupleReference[A]] with Serializable{
  def toIDBasedTupleReference: IDBasedTupleReference = {
    assert(table.getUnionedOriginalTables.size==1)
    matching.IDBasedTupleReference(table.getUnionedOriginalTables.head,rowIndex)
  }

  import scala.math.Ordering.Implicits._

  def getDataTuple = table.getDataTuple(rowIndex)

  override def compareTo(o: TupleReference[A]): Int ={
    val smaller = (table.getID,rowIndex) < (o.table.getID,o.rowIndex)
    val greater = (table.getID,rowIndex) > (o.table.getID,o.rowIndex)
    if(smaller) -1
    else if(greater) 1
    else 0
  }
}
