package de.hpi.wikipedia.statistics

import de.hpi.socrata.tfmp_input.table.nonSketch.{FactLineage, FactLineageWithHashMap}
import de.hpi.util.CSVUtil
import de.hpi.wikipedia.data.original.InfoboxRevisionHistory

import java.time.LocalDate
import scala.collection.mutable

case class WikipediaInfoboxStatisticsLine(template: Option[String], pageID: BigInt, key: String, p: String, lineage: FactLineageWithHashMap) {

  val fl = FactLineage.fromSerializationHelper(lineage).lineage
  val nonWcValues = getNonWCValuesInRange(fl)
  val totalRealChanges = getRealChangeCountInRange(fl)
  val nonWcValuesPerYear = WikipediaInfoboxStatisticsLine.years.map(i => getNonWCValuesInRange(fl.range(LocalDate.ofYearDay(i, 1), LocalDate.ofYearDay(i + 1, 1))))
  val realChangesPerYear = WikipediaInfoboxStatisticsLine.years.map( i => getRealChangeCountInRange(fl.range(LocalDate.ofYearDay(i,1),LocalDate.ofYearDay(i+1,1))))

  def getRealChangeCountInRange(fl: mutable.TreeMap[LocalDate, Any]) = {
    val withoutWildcard =fl
      .values
      .toIndexedSeq
      .filter(v => !FactLineage.isWildcard(v))
      .zipWithIndex
    withoutWildcard.filter{case (v,i) => i!=0 && v!=withoutWildcard(i-1)._1}.size
  }

  def getCSVLine = {
    (Seq(template.getOrElse(""),
      pageID,
      key,
      p,
      nonWcValues,
      totalRealChanges
    ) ++ nonWcValuesPerYear ++ realChangesPerYear).map(CSVUtil.toCleanString(_)).mkString(",")
  }

  private def getNonWCValuesInRange(fl: mutable.TreeMap[LocalDate, Any]) = {
    fl.values.filter(v => !FactLineage.isWildcard(v)).size
  }
}
object WikipediaInfoboxStatisticsLine{

  def getSchema = Seq("infoboxTemplate",
    "pageID",
    "infoboxKey",
    "property",
    "#totalNonWildcardValues",
    "#totalRealChanges") ++ years.map( i => s"#nonWildcardValuesInYear_${i}") ++ years.map( i => s"#realChangesInYear_${i}")

  val years = InfoboxRevisionHistory.EARLIEST_HISTORY_TIMESTAMP.getYear until InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP.getYear
}
