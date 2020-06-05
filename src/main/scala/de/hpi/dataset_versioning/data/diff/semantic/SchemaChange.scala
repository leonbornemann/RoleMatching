package de.hpi.dataset_versioning.data.diff.semantic

import de.hpi.dataset_versioning.util.TableFormatter

class SchemaChange() {

  var projection:Option[(Seq[String],Seq[String])] = None
  var columnInsert:Option[Seq[String]] = None

  def getProjectionString = if(projection.isDefined) TableFormatter.format( Seq( projection.get._1 ++ Seq("-->") ++ projection.get._2)) else ""
  def getColumnInsertString = if(columnInsert.isDefined) TableFormatter.format( Seq( columnInsert.get)) else ""

  def getAsPrintString =
    s"""Projection:
       |${getProjectionString}
       |Column Insert:
       |${getColumnInsertString}
       |""".stripMargin

}
