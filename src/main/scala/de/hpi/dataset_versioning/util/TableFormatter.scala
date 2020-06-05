package de.hpi.dataset_versioning.util

import scala.collection.mutable.ListBuffer

object TableFormatter {

  def format(table: collection.Seq[collection.Seq[Any]], fillMissing: Boolean): String = {
    if (fillMissing) {
      val newTable = fillMissingValues(table)
      format(newTable)
    } else
      format(table)
  }

  def addMissing(row: collection.Seq[Any], maxCols: Int): collection.Seq[Any] = {
    if (row.size == maxCols) {
      row
    } else {
      val newRow = ListBuffer[Any]()
      newRow.appendAll(row)
      while (newRow.size != maxCols) {
        newRow.append("NA")
      }
      newRow
    }
  }

  def fillMissingValues(table: collection.Seq[collection.Seq[Any]]) = {
    val newTable = ListBuffer[collection.Seq[Any]]()
    val maxCols = table.map(_.size).max
    for (row <- table) {
      newTable += addMissing(row, maxCols)
    }
    newTable
  }

  def format(table: collection.Seq[collection.Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val sizes = for (row <- table) yield (for (cell <- row) yield if (cell == null) 0 else cell.toString.length)
      val colSizes = for (col <- sizes.transpose) yield col.max
      val rows = for (row <- table) yield formatRow(row, colSizes)
      formatRows(rowSeparator(colSizes), rows)
  }

  def formatRows(rowSeparator: String, rows: collection.Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  def formatRow(row: collection.Seq[Any], colSizes: collection.Seq[Int]) = {
    val cells = (for ((item, size) <- row.zip(colSizes)) yield if (size == 0) "" else ("%" + size + "s").format(item))
    cells.mkString("|", "|", "|")
  }

  def rowSeparator(colSizes: collection.Seq[Int]) = colSizes map {
    "-" * _
  } mkString("+", "+", "+")
}