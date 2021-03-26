package de.hpi.tfm.compatibility.graph.fact

import de.hpi.tfm.compatibility.graph
import de.hpi.tfm.data.tfmp_input.table.TemporalDatabaseTableTrait

@SerialVersionUID(3L)
case class TupleReference[A](table: TemporalDatabaseTableTrait[A], rowIndex: Int) extends Comparable[TupleReference[A]] with Serializable {
  def toIDBasedTupleReference: IDBasedTupleReference = {
    assert(table.getUnionedOriginalTables.size == 1)
    IDBasedTupleReference(table.getUnionedOriginalTables.head, rowIndex)
  }

  import scala.math.Ordering.Implicits._

  def getDataTuple = table.getDataTuple(rowIndex)

  override def compareTo(o: TupleReference[A]): Int = {
    val smaller = (table.getID, rowIndex) < (o.table.getID, o.rowIndex)
    val greater = (table.getID, rowIndex) > (o.table.getID, o.rowIndex)
    if (smaller) -1
    else if (greater) 1
    else 0
  }
}
