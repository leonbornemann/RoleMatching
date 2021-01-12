package de.hpi.dataset_versioning.data.column_order

import de.hpi.dataset_versioning.data.column_order.ColumnOrderRestoreMain.MatchType.MatchType
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.{Attribute, RelationalDataset}
import de.hpi.dataset_versioning.db_synthesis.preparation.InteractiveOptimizationInputCompletion.subdomain
import de.hpi.dataset_versioning.io.IOService

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ColumnOrderRestoreMain extends App {

  IOService.socrataDir = args(0)
  val csvDir = args(1)
  val subdomain = if(args.size>2) Some(args(2)) else None
  var matchCounts = mutable.HashMap[MatchType.Value,Int]()

  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = if(subdomain.isDefined) Some(subDomainInfo(subdomain.get)
    .map(_.id)
    .toSet) else None

  def getBestPositionMatch(csvHeader: IndexedSeq[String], a: Attribute, originalIndex:Int) = {
    val exactMatch = csvHeader.indexOf(a.name)
    if(exactMatch!= -1)
      Match(originalIndex,exactMatch,MatchType.Exact)
    else {
      val reverseContainmentMatch = csvHeader.find(s => a.name.contains(s))
      if(reverseContainmentMatch.isDefined) {
        Match(originalIndex,csvHeader.indexOf(reverseContainmentMatch.get),MatchType.Reverse_Containment)
      } else
        Match(originalIndex,-1,MatchType.NoMatch)
    }
  }

  case class Match(originalIndex:Int,indexInCSV:Int, matchType: MatchType)

  object MatchType extends Enumeration {
    type MatchType = Value
    val Exact,Contaiment,Reverse_Containment,NoMatch = Value
  }

  def restoreColumnOrder(attributes: collection.IndexedSeq[Attribute], csvHeader: IndexedSeq[String]) = {
    //create mapping to csv
    val finalOrder = scala.collection.mutable.HashMap[Attribute,Int]()
    val positionToAttrGroup = scala.collection.mutable.TreeMap[Int,ArrayBuffer[(Attribute,Match)]]()
    attributes.zipWithIndex.foreach{ case (a,originalIndex) => {
      val bestMatch = getBestPositionMatch(csvHeader, a,originalIndex)
      matchCounts(bestMatch.matchType) = matchCounts.getOrElse(bestMatch.matchType,0)+1
      positionToAttrGroup.getOrElseUpdate(bestMatch.indexInCSV,ArrayBuffer[(Attribute,Match)]()).addOne((a,bestMatch))
      //positionToAttrGroup.
    }}
    var curPos = 0
    positionToAttrGroup
      .filter(_._1!= -1)
      .foreach{case (_,matches) => {
        val exactMatches = matches.filter(_._2.matchType==MatchType.Exact)
        val containmentMatches = matches.filter(_._2.matchType==MatchType.Reverse_Containment)
        assert(exactMatches.size+containmentMatches.size == matches.size)
        assert(exactMatches.size==1)
        finalOrder.put(exactMatches.head._1,curPos)
        curPos +=1
        containmentMatches.foreach(m => {
          finalOrder.put(m._1,curPos)
          curPos+=1
        })
      }}
    positionToAttrGroup.getOrElse(-1,ArrayBuffer())
      .foreach(m => {
        finalOrder.put(m._1,curPos)
        curPos+=1
      })
    assert(finalOrder.values.toIndexedSeq.sorted == (0 until attributes.size))
    assert(finalOrder.keySet == attributes.toSet)
    finalOrder
//    println("------------------------------------------")
//    println(csvHeader.sorted)
//    println(attributes.map(_.name).sorted)
//    println("------------------------------------------")
  }

  val resultFile = new PrintWriter("newColumnOrder.csv")
  resultFile.println("id,version,attributeID,oldSchemaPosition,newSchemaPosition")
  val files = new File(csvDir).listFiles()
    .filter(f => !subdomainIds.isDefined || subdomainIds.get.contains(f.getName.split("\\.")(0)))
  var processedCSVFiles = 0
  files.foreach(f => {
      val id = f.getName.split("\\.")(0)
      val versions = IOService.getAllSimplifiedDataVersionsForTimeRange(id,IOService.STANDARD_TIME_FRAME_START,LocalDate.parse("2020-11-01"))
        //.keySet.maxBy(_.toEpochDay)
      versions.keySet.foreach(v => {
        println(s"Processing $id version $v")
        val simplifiedDataTable = RelationalDataset.load(id,v)
        val csvHeader = firstLine(f).get
          .split(",")
          .toIndexedSeq
          .map(s => if(s.startsWith("\"")) s.substring(1,s.length-1) else s)
        val finalOrder = restoreColumnOrder(simplifiedDataTable.attributes,csvHeader)
        val originalOrder = simplifiedDataTable.attributes.zipWithIndex.toMap
        assert(finalOrder.keySet==originalOrder.keySet)
        finalOrder.foreach{case (a,finalPos) => {
          val originalPos = originalOrder(a)
          resultFile.println(s"$id,$v,${a.id},$originalPos,$finalPos")
        }}
      })
    processedCSVFiles+=1
    if(processedCSVFiles%100==0){
      println(s"Finished $processedCSVFiles csv files")
    }

//      val ts = TemporalSchema.load(id)
//      val attrs = ts.attributes.map(_.lastDefinedValue)
//      //val attrs = ts.attributes.flatMap(_.lineage.values.filter(_.exists).map(_.attr.get))
//      val csvHeader = firstLine(f).get
//        .split(",")
//        .toIndexedSeq
//      restoreColumnOrder(attrs,csvHeader)
    })
  resultFile.close()
  matchCounts.foreach(println(_))

  def firstLine(f: java.io.File): Option[String] = {
    val src = io.Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
    } finally {
      src.close()
    }
  }
}
