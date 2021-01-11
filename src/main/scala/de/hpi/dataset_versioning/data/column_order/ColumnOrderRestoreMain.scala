package de.hpi.dataset_versioning.data.column_order

import de.hpi.dataset_versioning.data.column_order.ColumnOrderRestoreMain.MatchType.MatchType
import de.hpi.dataset_versioning.data.metadata.custom.DatasetInfo
import de.hpi.dataset_versioning.data.metadata.custom.schemaHistory.TemporalSchema
import de.hpi.dataset_versioning.data.simplified.{Attribute, RelationalDataset}
import de.hpi.dataset_versioning.db_synthesis.preparation.InteractiveOptimizationInputCompletion.subdomain
import de.hpi.dataset_versioning.io.IOService

import java.io.File
import java.time.LocalDate
import scala.collection.mutable

object ColumnOrderRestoreMain extends App {

  IOService.socrataDir = args(0)
  val csvDir = args(1)
  val subdomain = if(args.size>2) Some(args(2)) else None
  var matchCounts = mutable.HashMap[MatchType.Value,Int]()

  val subDomainInfo = DatasetInfo.readDatasetInfoBySubDomain
  val subdomainIds = if(subdomain.isDefined) Some(subDomainInfo(subdomain.get)
    .map(_.id)
    .toSet) else None

  def getBestPositionMatch(csvHeader: IndexedSeq[String], a: Attribute) = {
    val exactMatch = csvHeader.indexOf(a.name)
    if(exactMatch!= -1)
      Match(exactMatch,MatchType.Exact)
    else {
      val containmentMatch = csvHeader.find(s => s.contains(a.name))
      if(containmentMatch.isDefined)
        Match(csvHeader.indexOf(containmentMatch.get),MatchType.Contaiment)
      else {
        val reverseContainmentMatch = csvHeader.find(s => a.name.contains(s))
        if(reverseContainmentMatch.isDefined) {
          Match(csvHeader.indexOf(reverseContainmentMatch.get),MatchType.Reverse_Containment)
        } else
          Match(-1,MatchType.NoMatch)
      }
    }
  }

  case class Match(index:Int,matchType: MatchType)

  object MatchType extends Enumeration {
    type MatchType = Value
    val Exact,Contaiment,Reverse_Containment,NoMatch = Value
  }

  def restoreColumnOrder(attributes: collection.IndexedSeq[Attribute], csvHeader: IndexedSeq[String]) = {
    //create mapping to csv
    val finalOrder = scala.collection.mutable.HashMap[Attribute,Int]()
    val positionToAttrGroup = scala.collection.mutable.HashMap[Int,(Attribute,Match)]()
    attributes.foreach(a => {
      val bestMatch = getBestPositionMatch(csvHeader, a)
      matchCounts(bestMatch.matchType) = matchCounts.getOrElse(bestMatch.matchType,0)+1
      //positionToAttrGroup.
    })
    println("------------------------------------------")
    println(csvHeader.sorted)
    println(attributes.map(_.name).sorted)
    println("------------------------------------------")
  }

  new File(csvDir).listFiles()
    .withFilter(f => !subdomainIds.isDefined || subdomainIds.get.contains(f.getName.split("\\.")(0)))
    .foreach(f => {
      val id = f.getName.split("\\.")(0)
      val lastVersion = IOService.getAllSimplifiedDataVersionsForTimeRange(id,IOService.STANDARD_TIME_FRAME_START,LocalDate.parse("2020-11-01"))
        .keySet.maxBy(_.toEpochDay)
      val simplifiedDataTable = RelationalDataset.load(id,lastVersion)
      val csvHeader = firstLine(f).get
        .split(".")
        .toIndexedSeq
        .map(s => if(s.startsWith("\"")) s.substring(1,s.length-1) else s)
      restoreColumnOrder(simplifiedDataTable.attributes,csvHeader)

//      val ts = TemporalSchema.load(id)
//      val attrs = ts.attributes.map(_.lastDefinedValue)
//      //val attrs = ts.attributes.flatMap(_.lineage.values.filter(_.exists).map(_.attr.get))
//      val csvHeader = firstLine(f).get
//        .split(",")
//        .toIndexedSeq
//      restoreColumnOrder(attrs,csvHeader)
    })
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
