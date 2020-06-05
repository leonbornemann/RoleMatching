package de.hpi.dataset_versioning.data.exploration

import java.io.{File, PrintWriter}

import com.google.gson.{JsonElement, JsonPrimitive}
import de.hpi.dataset_versioning.data.LoadedRelationalDataset
import de.hpi.dataset_versioning.data.diff.semantic.{DiffSimilarity, RelationalDatasetDiff}
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable
import scala.io.Source

class DatasetHTMLExporter() {

  val diffStyleDelete = "<div style=\"color:red;text-decoration:line-through;resize:none;\">"
  val diffStyleInsert = "<div style=\"color:green;resize:none;\">"
  val diffStyleUpdate = "<div style=\"color:blue;resize:none;\">"
  val diffStyleValuesInBoth = "<div style=\"color:gold;resize:none;\">"
  val diffStyleNormal = "<div style=\"resize:none;\">"

  val idToLineageMap = IOService.readCleanedDatasetLineages()
    .map(l => (l.id,l))
    .toMap

  def exportDiffMetadataToHTMLPrinter(ds1: LoadedRelationalDataset,ds2:LoadedRelationalDataset, pr: PrintWriter) = {
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
    pr.println(escapeHTML(s"${idToLineageMap(ds1.id).versionsWithChanges.size}"))
    pr.println("&emsp;")
    pr.println("<b>#DeletionsOfDataset: </b>")
    pr.println(escapeHTML(s"${idToLineageMap(ds1.id).deletions.size}"))
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

  def exportDiffToHTMLPrinter(dsBeforeChange: LoadedRelationalDataset, dsAfterChange: LoadedRelationalDataset, diff: RelationalDatasetDiff,diffOverlap:DiffSimilarity, pr: PrintWriter) = {
    //very simple schema based col-matching
    val colDeletes = dsBeforeChange.colNames.toSet.diff(dsAfterChange.colNames.toSet)
    val colInserts = dsAfterChange.colNames.toSet.diff(dsBeforeChange.colNames.toSet)
    val colMatches = dsBeforeChange.colNames.toSet.intersect(dsAfterChange.colNames.toSet)
    val resultColSet = dsBeforeChange.colNames.toSet.union(dsAfterChange.colNames.toSet)
        .toIndexedSeq
        .sorted
    pr.println("<tr>")
    resultColSet.foreach(cell => {
      if(colMatches.contains(cell)) {
        pr.println("<th><div>")
        pr.println(escapeHTML(cell))
        pr.println("</div></th>")
      } else if(colDeletes.contains(cell)){
        pr.println("<th>" + diffStyleDelete)
        pr.println(escapeHTML(cell))
        pr.println("</div></th>")
      } else{
        assert(colInserts.contains(cell))
        pr.println("<th><div style=\"color:green;\">")
        pr.println(escapeHTML(cell))
        pr.println("</div></th>")
      }
    })
    pr.println("</tr>")
    val resultingColOrder = resultColSet.zipWithIndex.toMap
    //TODO: think about tuple order?
    //deleted first! - then inserts - then updates
    addUpdatedTuplesToHTMLTable(diff.updates,resultingColOrder,diffOverlap,pr)
    addToHTMLTable(diff.inserts, diffStyleInsert, resultingColOrder,diffOverlap,pr)
    addToHTMLTable(diff.deletes, diffStyleDelete, resultingColOrder,diffOverlap,pr)
    addToHTMLTable(diff.unchanged.take(10),diffStyleNormal,resultingColOrder,diffOverlap,pr)
    val dummyRow:mutable.HashSet[Set[(String, JsonElement)]] = mutable.HashSet(resultingColOrder.keySet.map(t => (t, new JsonPrimitive("..."))))
    addToHTMLTable(dummyRow,diffStyleNormal,resultingColOrder,diffOverlap,pr)
  }

  def addUpdatedTuplesToHTMLTable(updates: mutable.HashMap[Set[(String, JsonElement)], Set[(String, JsonElement)]],
                                  resultingColOrder: Map[String, Int],diffOverleap:DiffSimilarity, pr: PrintWriter) = {
    updates.foreach { case(from,to) => {
      val fromMap = from.toMap
      val toMap = to.toMap
      pr.println("<tr>")
      val htmlCells = mutable.ArrayBuffer[String]() ++= Seq.fill[String](resultingColOrder.size)("-")
      resultingColOrder.foreach { case (cname, curCellIndex) => {
        var curCell = "-"
        val intersectionDiffP = "<p style=\"color:gold;\">"
        val sb = new mutable.StringBuilder()
        sb.append("<td>")
        if(fromMap.contains(cname) && toMap.contains(cname)){
          //we have an update:
          val prevValue = fromMap(cname)
          val newValue = toMap(cname)
          if(prevValue!=newValue) sb.append(diffStyleUpdate) else sb.append(diffStyleNormal)
          appendFormattedOldCellValue(diffOverleap, intersectionDiffP, sb, prevValue)
          sb.append(escapeHTML(" --> "))
          appendFormattedNewValue(diffOverleap, intersectionDiffP, sb, newValue)
        } else if(fromMap.contains(cname)){
          val prevValue = fromMap(cname)
          sb.append(diffStyleDelete)
          appendFormattedOldCellValue(diffOverleap, intersectionDiffP, sb, prevValue)
        } else{
          assert(toMap.contains(cname))
          val newValue = toMap(cname)
          appendFormattedNewValue(diffOverleap, intersectionDiffP, sb, newValue)
        }
        sb.append("</div></td>")
        curCell = sb.toString()
        htmlCells(curCellIndex) = curCell
      }}
      htmlCells.foreach(c => {
        pr.println(c)
      })
      pr.println("</tr>")
    }
    }

  }

  private def appendFormattedOldCellValue(diffOverleap: DiffSimilarity, intersectionDiffP: String, sb: StringBuilder, prevValue: JsonElement) = {
    if (diffOverleap.oldValueOverlap.contains(prevValue))
      sb.append(intersectionDiffP)
    sb.append(escapeHTML(LoadedRelationalDataset.getCellValueAsString(prevValue)))
    if (diffOverleap.oldValueOverlap.contains(prevValue))
      sb.append("</p>")
  }

  private def appendFormattedNewValue(diffOverleap: DiffSimilarity, intersectionDiffP: String, sb: StringBuilder, newValue: JsonElement) = {
    if (diffOverleap.newValueOverlap.contains(newValue))
      sb.append(intersectionDiffP)
    sb.append(escapeHTML(LoadedRelationalDataset.getCellValueAsString(newValue)))
    if (diffOverleap.newValueOverlap.contains(newValue))
      sb.append("</p>")
  }

  private def addToHTMLTable(rows:  mutable.HashSet[Set[(String, JsonElement)]], standardDiffStyle: String, resultingColOrder: Map[String, Int], diffOverlap:DiffSimilarity, pr:PrintWriter) = {
    rows.foreach { tupleSet => {
      pr.println("<tr>")
      val htmlCells = mutable.ArrayBuffer[String]() ++= Seq.fill[String](resultingColOrder.size)("-")
      tupleSet.foreach { case (cname, e) => {
        val curCellIndex = resultingColOrder(cname)
        val cell = new mutable.StringBuilder()
        cell.append("<td>" + standardDiffStyle)
        if(diffOverlap.newValueOverlap.contains(e) || diffOverlap.oldValueOverlap.contains(e))
          cell.append("<p style=\"color:gold;\">")
        cell.append(escapeHTML(LoadedRelationalDataset.getCellValueAsString(e)))
        if(diffOverlap.newValueOverlap.contains(e) || diffOverlap.oldValueOverlap.contains(e))
          cell.append("</p>")
        cell.append("</div></td>")
        htmlCells(curCellIndex) = cell.toString()
      }
      }
      htmlCells.foreach(c => {
        pr.println(c)
      })
      pr.println("</tr>")
    }
    }
  }

  def exportDiffTableView(dsBeforeChange: LoadedRelationalDataset, dsAfterChange: LoadedRelationalDataset, diff: RelationalDatasetDiff, outFile: File) = {
    val template = "/html_output_templates/ScollableTableTemplate.html"
    val pr = new PrintWriter(outFile)
    val is = getClass.getResourceAsStream(template)
    Source.fromInputStream(is)
      .getLines()
      .foreach( l => {
        if(l.startsWith("????")){
          if(l.contains("METAINFO"))
            exportDiffMetadataToHTMLPrinter(dsBeforeChange,dsAfterChange,pr)
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

  def exportDiffPairToTableView(dsABeforeChange: LoadedRelationalDataset,
                                dsAAfterChange: LoadedRelationalDataset,
                                diffA: RelationalDatasetDiff,
                                dsBBeforeChange: LoadedRelationalDataset,
                                dsBAfterChange: LoadedRelationalDataset,
                                diffB: RelationalDatasetDiff,
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
          if(l.contains("METAINFO1"))
            exportDiffMetadataToHTMLPrinter(dsABeforeChange,dsAAfterChange,pr)
          else if(l.contains("TABLECONTENT1"))
            exportDiffToHTMLPrinter(dsABeforeChange,dsAAfterChange,diffA,diffSimilarity,pr)
          if(l.contains("METAINFO2"))
            exportDiffMetadataToHTMLPrinter(dsBBeforeChange,dsBAfterChange,pr)
          else if(l.contains("TABLECONTENT2"))
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

  def exportToHTMLPrinter(ds:LoadedRelationalDataset,pr: PrintWriter) = {
    pr.println(toHTMLTableRow(ds.colNames,"th",true))
    ds.rows.foreach(r => {
      pr.println(toHTMLTableRow(r,"td",false))
    })
  }

  def exportMetadataToHTMLPrinter(ds: LoadedRelationalDataset, pr: PrintWriter) = {
    IOService.cacheMetadata(ds.version)
    val md = IOService.cachedMetadata(ds.version)(ds.id)
    pr.println("<p><div style=\"width:100%;overflow:auto;\">" + escapeHTML(s"ID: ${ds.id}") + "<br/>" +
      escapeHTML(s"Version: ${ds.version.format(IOService.dateTimeFormatter)}")+ "<br/>" +
      escapeHTML(s"Name: ${md.resource.name}")+ "<br/>" +
      escapeHTML(s"URL: ${md.link}") + "<br/>" +
      escapeHTML(s"Description: ${md.resource.description.getOrElse("-")}") + "</p></div>")
  }

  def toHTML(ds:LoadedRelationalDataset, outFile:File) = {
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

}
