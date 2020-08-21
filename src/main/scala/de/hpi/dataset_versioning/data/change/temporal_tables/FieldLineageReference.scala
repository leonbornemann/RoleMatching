package de.hpi.dataset_versioning.data.change.temporal_tables

import de.hpi.dataset_versioning.data.change.{FieldLineageCompatibility, ReservedChangeValues}

case class FieldLineageReference(table: TemporalTable, rowIndex: Int, colIndex: Int) {

  val insertTime = calculateInsertTime //the time of the first value that is not non-existant

  def calculateInsertTime = {
    val valuesOrdered = lineage.lineage.iterator
    var curElem = valuesOrdered.next()
    while (curElem._2 == ReservedChangeValues.NOT_EXISTANT_ROW) {
      if (!valuesOrdered.hasNext)
        throw new AssertionError("Sequence of only non-existant values found")
      curElem = valuesOrdered.next()
    }
    curElem._1
  }

  def lineage = table.rows(rowIndex).fields(colIndex)


  def isComptabileTo(other: FieldLineageReference, method: FieldLineageCompatibility) = method.isCompatible(this, other)

}
