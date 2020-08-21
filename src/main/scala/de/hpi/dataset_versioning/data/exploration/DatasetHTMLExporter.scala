package de.hpi.dataset_versioning.data.exploration

import java.io.{File, PrintWriter}

import com.google.gson.{JsonElement, JsonPrimitive}
import de.hpi.dataset_versioning.data.OldLoadedRelationalDataset
import de.hpi.dataset_versioning.data.change.{Change, DiffAsChangeCube}
import de.hpi.dataset_versioning.data.diff.semantic.DiffSimilarity
import de.hpi.dataset_versioning.data.history.DatasetVersionHistory
import de.hpi.dataset_versioning.data.simplified.{Attribute, RelationalDataset, RelationalDatasetRow}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.io.Source

class DatasetHTMLExporter() {

  val diffStyleDelete = "<div style=\"color:red;text-decoration:line-through;resize:none;\">"
  val diffStyleInsert = "<div style=\"color:blue;resize:none;\">"
  val diffStyleUpdate = "<div style=\"color:blue;resize:none;\">"
  val diffStyleValuesInBoth = "<div style=\"color:green;font-weight: bold;resize:none;\">"
  val diffStyleNormal = "<div style=\"resize:none;\">"

  val idToLineageMap = IOService.readCleanedDatasetLineages()
    .map(l => (l.id,l))
    .toMap

  def exportDiffMetadataToHTMLPrinter(ds1: RelationalDataset, ds2:RelationalDataset, pr: PrintWriter) = {
    IOService.cacheMetadata(ds1.version)
    IOService.cacheMetadata(ds2.version)
    val md1 = IOService.cachedMetadata(ds1.version)(ds1.id)
    val md2 = IOService.cachedMetadata(ds2.version)(ds2.id)
    //opening Div:
    pr.println("<p><div style=\"width:100%;overflow:auto;height:200px\">")
    //ID and lineage size
    pr.println("<b>ID: </b>")
    pr.println(escapeHTML(s"${ds1.id}"))
    pr.println("&emsp;")
    pr.println("<b>#VersionsOfDataset: </b>")
    pr.println(escapeHTML(s"${idToLineageMap.getOrElse(ds1.id,new DatasetVersionHistory(ds1.id)).versionsWithChanges.size}"))
    pr.println("&emsp;")
    pr.println("<b>#DeletionsOfDataset: </b>")
    pr.println(escapeHTML(s"${idToLineageMap.getOrElse(ds1.id,new DatasetVersionHistory(ds1.id)).deletions.size}"))
    pr.println("<br/>")
    //Version:
    //pr.println("<p><div style=\"width:100%;overflow:auto;\">")
    pr.println("<b>Version: </b>")
    pr.println(escapeHTML(s"${ds1.version.format(IOService.dateTimeFormatter)} --> ${ds2.version.format(IOService.dateTimeFormatter)}"))
    pr.println("<br/>")
    //Name
    val nameStr = if(md1.resource.name == md2.resource.name) md1.resource.name else s"${md1.resource.name} --> ${md2.resource.name}"
    //pr.println("<p><div style=\"width:100%;overflow:auto;\">")
    pr.println("<b>Name: </b>")
    pr.println(escapeHTML(s"$nameStr"))
    pr.println("<br/>")
    //Link
    val linkStr = if(md1.link == md2.link) md1.link else s"${md1.link} --> ${md2.link}"
    //pr.println("<p><div style=\"width:100%;overflow:auto;\">")
    pr.println("<b>Link: </b>")
    pr.println(escapeHTML(s"$linkStr"))
    pr.println("<br/>")
    val descrStr = if(md1.resource.description.getOrElse("-") == md2.resource.description.getOrElse("-")) md1.resource.description.getOrElse("-") else s"${md1.resource.description.getOrElse("-")} --> ${md2.resource.description.getOrElse("-")}"
    //pr.println("<p><div style=\"width:100%;overflow:auto;\">")
    pr.println("<b>Description: </b>")
    pr.println(escapeHTML(s"$descrStr"))
    pr.println("<br/>")
    //closing div
    pr.println("</div></p>")
  }

  def exportDiffToHTMLPrinter(dsBeforeChange: RelationalDataset, dsAfterChange: RelationalDataset, diff: DiffAsChangeCube, diffOverlap:DiffSimilarity, pr: PrintWriter) = {
    //very simple schema based col-matching
    val resultColSet = diff.colIdToAppearance
      .toIndexedSeq
      .sortBy{case (_,(prev,new_)) => {
        if(!new_.isDefined) {
          assert(diff.columnDeletes.contains(prev.get))
          "\u1D958" + prev.get.name //a somewhat hacky solution that should guarantee to push all deleted columns to the end
        } else if(!prev.isDefined){
          assert(diff.insertedColumns.contains(new_.get))
          "\u1D957" + new_.get.name //a somewhat hacky solution that should guarantee to push all inserted columns to the end but before deletes
        }
        else new_.get.name
      }}
    pr.println("<tr>")
    resultColSet.foreach{case (id,(prev,cur)) => {
      if(prev.isDefined && cur.isDefined) {
        val colnameStr = if(prev.get.name == cur.get.name) prev.get.name else prev.get.name + " --> " + cur.get.name
        pr.println("<th><div>")
        pr.println(escapeHTML(colnameStr))
        pr.println("</div></th>")
      } else if(!cur.isDefined){
        pr.println("<th>" + diffStyleDelete)
        pr.println(escapeHTML(prev.get.name))
        pr.println("</div></th>")
      } else{
        pr.println("<th>" + diffStyleInsert)
        pr.println(escapeHTML(cur.get.name))
        pr.println("</div></th>")
      }
    }}
    pr.println("</tr>")
    val resultingColOrder = resultColSet.map(_._1).zipWithIndex.toMap
    addUpdatedTuplesToHTMLTable(diff,resultingColOrder,diffOverlap,pr)
    val dummyRow:IndexedSeq[RelationalDatasetRow] = IndexedSeq(new RelationalDatasetRow(-1,resultingColOrder.keySet.map(_ => "...").toIndexedSeq))
    addToHTMLTable(dummyRow,diff.v1.attributes,diffStyleNormal,resultingColOrder,diffOverlap,pr)
  }



  def addUpdatedTuplesToHTMLTable(diff:DiffAsChangeCube, resultingColOrder: Map[Int, Int], diffOverleap:DiffSimilarity, pr: PrintWriter) = {
    //at the end:
    ??? //If this method is needed again fix the code below to use single change list instead of individual lists:
//    val byRowAndCol = (diff.changeCube.allChanges)
//      .groupBy(c => (c.e,c.pID))
//        .map{case(k,v) => {
//          if(v.size>1) throw new AssertionError("inconsistent change cube")
//          (k,v.head)
//        }}
//    object RowOrdering extends Ordering[RelationalDatasetRow] {
//      def compare(a: RelationalDatasetRow, b: RelationalDatasetRow) = {
//        val rowSummaryA = RowSummary.getRowSummary(resultingColOrder,a,byRowAndCol,diffOverleap)
//        val rowSummaryB = RowSummary.getRowSummary(resultingColOrder,b,byRowAndCol,diffOverleap)
//        val result = rowSummaryA.compareTo(rowSummaryB)
//        result
//      }
//    }
//    val deletedColIDs = diff.columnDeletes.map(_.id)
//    val sortedRows = diff.v2.rows.sorted(RowOrdering)
//    sortedRows.foreach(r => {
//    //updates.foreach { case(from,to) => {
//      pr.println("<tr>")
//      val htmlCells = mutable.ArrayBuffer[String]() ++= Seq.fill[String](resultingColOrder.size)("-")
//      if(diff.v1.id == "test-0002" && r.id==3)
//        print()
//      resultingColOrder.foreach { case (colID, colIndex) => {
//        var curCell = "-"
//        val intersectionDiffP = "<p style=\"color:green;font-weight: bold;\">"
//        val sb = new mutable.StringBuilder()
//        sb.append("<td>")
//        val associatedChange = byRowAndCol.get((r.id,colID))
//        if(colIndex==3)
//          print()
//        if(associatedChange.isDefined && associatedChange.get.isUpdate){
//          //we have an update:
//          val prevValue = associatedChange.get.prevValue
//          val newValue = associatedChange.get.newValue
//          if(prevValue!=newValue) sb.append(diffStyleUpdate) else sb.append(diffStyleNormal)
//          appendFormattedOldCellValue(diffOverleap, intersectionDiffP, sb, prevValue.toString)
//          sb.append(escapeHTML(" --> "))
//          appendFormattedNewValue(diffOverleap, intersectionDiffP, sb, newValue.toString)
//        } else if(associatedChange.isDefined && associatedChange.get.isDelete){
//          val prevValue = associatedChange.get.prevValue
//          sb.append(diffStyleDelete)
//          appendFormattedOldCellValue(diffOverleap, intersectionDiffP, sb, prevValue)
//        } else if(associatedChange.isDefined && associatedChange.get.isInsert) {
//          val curValue = associatedChange.get.newValue
//          sb.append(diffStyleInsert)
//          appendFormattedNewValue(diffOverleap, intersectionDiffP, sb, curValue)
//        } else if(!deletedColIDs.contains(colID)){
//          //we need to recognize if the column was deleted
//          sb.append(diffStyleNormal)
//          appendFormattedNewValue(diffOverleap, intersectionDiffP, sb, r.fields(colIndex))
//        } else{
//          sb.append(diffStyleNormal)
//        }
//        sb.append("</div></td>")
//        curCell = sb.toString()
//        htmlCells(colIndex) = curCell
//      }}
//      htmlCells.foreach(c => {
//        pr.println(c)
//      })
//      pr.println("</tr>")
//    })
//
//    val deletedRowIds = diff.entireRowDeletes
//    val deletedRows = diff.v1.rows
//        .filter(r => deletedRowIds.contains(r.id))
//    addToHTMLTable(deletedRows,diff.v1.attributes,diffStyleDelete, resultingColOrder,diffOverleap,pr)

  }

  private def appendFormattedOldCellValue(diffOverleap: DiffSimilarity, intersectionDiffP: String, sb: StringBuilder, prevValue: Any) = {
    if (diffOverleap.oldValueOverlap.contains(prevValue))
      sb.append(intersectionDiffP)
    sb.append(escapeHTML(prevValue.toString))
    if (diffOverleap.oldValueOverlap.contains(prevValue))
      sb.append("</p>")
  }

  private def appendFormattedNewValue(diffOverleap: DiffSimilarity, intersectionDiffP: String, sb: StringBuilder, newValue: Any) = {
    if (diffOverleap.newValueOverlap.contains(newValue))
      sb.append(intersectionDiffP)
    sb.append(escapeHTML(newValue.toString))
    if (diffOverleap.newValueOverlap.contains(newValue))
      sb.append("</p>")
  }

  private def addToHTMLTable(rows:  collection.IndexedSeq[RelationalDatasetRow], attributes:collection.IndexedSeq[Attribute], standardDiffStyle: String, resultingColOrder: Map[Int, Int], diffOverlap:DiffSimilarity, pr:PrintWriter) = {
    rows.foreach { row => {
      pr.println("<tr>")
      val htmlCells = mutable.ArrayBuffer[String]() ++= Seq.fill[String](resultingColOrder.size)("-")
      for (i<- 0 until(row.fields.size)){
      //row.foreach { case (cname, e) => {
        val e = row.fields(i)
        val curCellIndex = resultingColOrder(attributes(i).id)
        val cell = new mutable.StringBuilder()
        cell.append("<td>" + standardDiffStyle)
        if(diffOverlap.newValueOverlap.contains(e) || diffOverlap.oldValueOverlap.contains(e))
          cell.append("<p style=\"color:gold;\">")
        cell.append(escapeHTML(e.toString))
        if(diffOverlap.newValueOverlap.contains(e) || diffOverlap.oldValueOverlap.contains(e))
          cell.append("</p>")
        cell.append("</div></td>")
        htmlCells(curCellIndex) = cell.toString()
      }
      htmlCells.foreach(c => {
        pr.println(c)
      })
      pr.println("</tr>")
    }
    }
  }

  def exportDiffTableView(dsBeforeChange: RelationalDataset, dsAfterChange: RelationalDataset, diff: DiffAsChangeCube, outFile: File) = {
    val template = "/html_output_templates/ScollableTableTemplate.html"
    val pr = new PrintWriter(outFile)
    val is = getClass.getResourceAsStream(template)
    Source.fromInputStream(is)
      .getLines()
      .foreach( l => {
        if(l.startsWith("????")){
          if(l.contains("METAINFO")) {
            println()
            //exportDiffMetadataToHTMLPrinter(dsBeforeChange,dsAfterChange,pr)
          }
          else if(l.contains("TABLECONTENT"))
            exportDiffToHTMLPrinter(dsBeforeChange,dsAfterChange,diff,DiffSimilarity(),pr)
        } else
          pr.println(l)
      })
    pr.close()
  }

  def exportCorrelationInfoToHTMLPrinter(dp: ChangeCorrelationInfo,idLeft:String, diffSimilarity:DiffSimilarity, pr: PrintWriter) = {
    pr.println("<table>")
    pr.println("<tr>")
    addValueToTableCell("Top-Level Domain:  ",dp.domain, pr)
    if(idLeft==dp.idA){
      //a before b
      addValueToTableCell("A: ",dp.idA, pr)
      addValueToTableCell("P(A): ",dp.P_A, pr)
    }
    addValueToTableCell("B: ",dp.idB, pr)
    addValueToTableCell("P(B): ",dp.P_B, pr)
    if(idLeft!=dp.idA){
      //b before A
      addValueToTableCell("A: ",dp.idA, pr)
      addValueToTableCell("P(A): ",dp.P_A, pr)
    }
    pr.println("<table>")
    pr.println("<tr>")
    addValueToTableCell("P(A AND B): ",dp.P_A_AND_B, pr)
    addValueToTableCell("P(A|B): ",dp.P_A_IF_B, pr)
    addValueToTableCell("P(B|A): ",dp.P_B_IF_A, pr)
    //diff similarities:
    addValueToTableCell("PSchemaChangeSim: ",diffSimilarity.schemaSimilarity, pr)
    addValueToTableCell("NewValSim: ",diffSimilarity.newValueSimilarity, pr)
    addValueToTableCell("DeletedValSim: ",diffSimilarity.deletedValueSimilarity, pr)
    addValueToTableCell("UpdateSim: ",diffSimilarity.fieldUpdateSimilarity, pr)
    pr.println("</tr>")
    pr.println("</table>")
  }

  private def addValueToTableCell(vDescription:String,v: Any, pr: PrintWriter) = {
    pr.println("<td>")
    pr.println(s"<b>${escapeHTML(vDescription)}</b>")
    pr.println(escapeHTML(s"$v"))
    pr.println("</td>")
  }

  def exportDiffPairToTableView(dsABeforeChange: RelationalDataset,
                                dsAAfterChange: RelationalDataset,
                                diffA: DiffAsChangeCube,
                                dsBBeforeChange: RelationalDataset,
                                dsBAfterChange: RelationalDataset,
                                diffB: DiffAsChangeCube,
                                dp:ChangeCorrelationInfo,
                                diffSimilarity:DiffSimilarity,
                                outFile: File) = {
    val template = "/html_output_templates/ScrollableTableTemplateForJointDiff.html"
    val pr = new PrintWriter(outFile)
    val is = getClass.getResourceAsStream(template)
    Source.fromInputStream(is)
      .getLines()
      .foreach( l => {
        if(l.trim.startsWith("????")){
          if(l.contains("CORRELATIONMETAINFO"))
            exportCorrelationInfoToHTMLPrinter(dp,dsAAfterChange.id,diffSimilarity,pr)
          if(l.contains("METAINFO1")) {
            println()
            exportDiffMetadataToHTMLPrinter(dsABeforeChange,dsAAfterChange,pr)
          } else if(l.contains("TABLECONTENT1"))
            exportDiffToHTMLPrinter(dsABeforeChange,dsAAfterChange,diffA,diffSimilarity,pr)
          if(l.contains("METAINFO2")) {
            println()
            exportDiffMetadataToHTMLPrinter(dsBBeforeChange,dsBAfterChange,pr)
          } else if(l.contains("TABLECONTENT2"))
            exportDiffToHTMLPrinter(dsBBeforeChange,dsBAfterChange,diffB,diffSimilarity,pr)
        } else
          pr.println(l)
      })
    pr.close()
  }


  def escapeHTML(s:String): String = {
    val sb = new mutable.StringBuilder()
    s.foreach(c => {
      c match {
        case '&' => sb.append("&amp;")
        case '<' => sb.append("&lt;")
        case '>' => sb.append("&gt;")
        case '"' => sb.append("&quot;")
        case '\'' => sb.append("&#39;")
        case c => sb.append(c)
      }
    })
    sb.toString()
  }

  def toHTMLTableRow(row: collection.IndexedSeq[Any], cellTag:String,useDiv:Boolean) = {
    val divStrOpen = if(useDiv) "<div>" else ""
    val divStrClose = if(useDiv) "</div>" else ""
    s"<tr>\n<$cellTag>$divStrOpen${row.map(c => escapeHTML(c.toString)).mkString(s"$divStrClose</$cellTag>\n<$cellTag>$divStrOpen")}$divStrClose</$cellTag>\n</tr>"
  }

  def exportToHTMLPrinter(ds:OldLoadedRelationalDataset, pr: PrintWriter) = {
    pr.println(toHTMLTableRow(ds.colNames,"th",true))
    ds.rows.foreach(r => {
      pr.println(toHTMLTableRow(r,"td",false))
    })
  }

  def exportMetadataToHTMLPrinter(ds: OldLoadedRelationalDataset, pr: PrintWriter) = {
    IOService.cacheMetadata(ds.version)
    val md = IOService.cachedMetadata(ds.version)(ds.id)
    pr.println("<p><div style=\"width:100%;overflow:auto;\">" + escapeHTML(s"ID: ${ds.id}") + "<br/>" +
      escapeHTML(s"Version: ${ds.version.format(IOService.dateTimeFormatter)}")+ "<br/>" +
      escapeHTML(s"Name: ${md.resource.name}")+ "<br/>" +
      escapeHTML(s"URL: ${md.link}") + "<br/>" +
      escapeHTML(s"Description: ${md.resource.description.getOrElse("-")}") + "</p></div>")
  }

  def toHTML(ds:OldLoadedRelationalDataset, outFile:File) = {
    val template = "src/main/resources/html_output_templates/ScollableTableTemplate.html"
    val pr = new PrintWriter(outFile)
    Source.fromFile(template)
      .getLines()
      .foreach( l => {
        if(l.startsWith("????")){
          if(l.contains("METAINFO"))
            exportMetadataToHTMLPrinter(ds,pr)
          else if(l.contains("TABLECONTENT"))
            exportToHTMLPrinter(ds,pr)
        } else
          pr.println(l)
      })
    pr.close()
  }

  case class RowSummary(overlappingUpdates: Int, overlappingInserts: Int, overlappingDeletes: Int, updates: Int, inserts: Int, deletes: Int) {

    def compareTo(b: RowSummary): Int = {
      val seq = Seq(overlappingUpdates,overlappingInserts,overlappingDeletes,updates,inserts,deletes)
        .zip(Seq(b.overlappingUpdates,b.overlappingInserts,b.overlappingDeletes,b.updates,b.inserts,b.deletes))
        .map{case (i,j) =>  j.compareTo(i)}
        .filter(i => i!=0)
      if(seq.isEmpty) 0
      else seq.head
    }

    def +(b: RowSummary) = RowSummary(overlappingUpdates+b.overlappingUpdates,
      overlappingInserts + b.overlappingInserts,
      overlappingDeletes + b.overlappingDeletes,
      updates + b.updates,
      inserts + b.inserts,
      deletes + b.deletes)

  }
  object RowSummary {
    def getRowSummary(resultingColOrder:Map[Int,Int],row:RelationalDatasetRow,changeByRowAndCol:Map[(Long,Int),Change],diffOverleap:DiffSimilarity):RowSummary = {
      ??? //If this method is needed again fix the code below to use single change list instead of individual lists:
//      resultingColOrder.map { case (colID, _) => {
//        val associatedChange = changeByRowAndCol.get((row.id, colID))
//        if (!associatedChange.isDefined)
//          RowSummary(0, 0, 0, 0, 0, 0)
//        else if (associatedChange.get.isUpdate && diffOverleap.updateOverlap.contains(associatedChange.get.getValueTuple))
//          RowSummary(1, 0, 0, 0, 0, 0)
//        else if (associatedChange.get.isInsert && diffOverleap.newValueOverlap.contains(associatedChange.get.newValue))
//          RowSummary(0, 1, 0, 0, 0, 0)
//        else if (associatedChange.get.isDelete && diffOverleap.oldValueOverlap.contains(associatedChange.get.prevValue))
//          RowSummary(0, 0, 1, 0, 0, 0)
//        else if (associatedChange.get.isUpdate)
//          RowSummary(0, 0, 0, 1, 0, 0)
//        else if (associatedChange.get.isInsert)
//          RowSummary(0, 0, 0, 0, 1, 0)
//        else if (associatedChange.get.isDelete)
//          RowSummary(0, 0, 0, 0, 0, 1)
//        else
//          throw new AssertionError("we should not get here :)")
//      }
//      }.reduce((a, b) => a + b)
    }
  }

}
