package de.hpi.tfm.data.wikipedia.infobox.transformed

import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, FactLineageWithHashMap, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.tfm.data.wikipedia.infobox.original.InfoboxRevisionHistory
import de.hpi.tfm.data.wikipedia.infobox.statistics.vertex.WikipediaInfoboxStatisticsLine
import de.hpi.tfm.evaluation.data.IdentifiedFactLineage
import de.hpi.tfm.fact_merging.config.UpdateChangeCounter

import java.time.{LocalDate, Period}

case class WikipediaInfoboxValueHistory(template:Option[String],
                                        pageID: BigInt,
                                        key: String,
                                        p: String,
                                        lineage: FactLineageWithHashMap) extends JsonWritable[WikipediaInfoboxValueHistory]{
  def isOfInterest = {
    val statLine = toWikipediaInfoboxStatisticsLine
    val nonWildcardPeriod = lineage.toFactLineage.nonWildcardDuration(InfoboxRevisionHistory.LATEST_HISTORY_TIMESTAMP)
    val hasRealChange = statLine.totalRealChanges >= 1
    val hasEnoughNonWildcard = nonWildcardPeriod.getDays >= Period.ofYears(1).getDays
    hasRealChange && hasEnoughNonWildcard
  }

  def changeCount = {
    val withIndex = lineage.toFactLineage.lineage
      .filter(t => !FactLineage.isWildcard(t._2))
      .toIndexedSeq
      .zipWithIndex
    withIndex
      //.withFilter{case ((t,v),i) => !FactLineage.isWildcard(v)}
      .filter{case ((t,v),i) => i==0 || v != withIndex(i-1)}
      .size-1
  }

  def toWikipediaURLInfo = s"https://en.wikipedia.org/?curid=$pageID ($p)"

  def projectToTimeRange(start: LocalDate, end: LocalDate) = {
    WikipediaInfoboxValueHistory(template,pageID,key,p,lineage.toFactLineage.projectToTimeRange(start,end).toSerializationHelper)
  }

  def toGeneralFactLineage = {
    val wikipediaID = WikipediaInfoboxPropertyID(template,pageID,key,p)
    IdentifiedFactLineage(wikipediaID.toCompositeID,lineage)
  }

  def toWikipediaInfoboxStatisticsLine = {
    WikipediaInfoboxStatisticsLine(template,pageID,key,p,lineage)
  }

}

object WikipediaInfoboxValueHistory extends JsonReadable[WikipediaInfoboxValueHistory]{

  def toAssociationTable(histories: IndexedSeq[WikipediaInfoboxValueHistory], id:AssociationIdentifier,attrID:Int) = {
    //id:String,
    //                                                                unionedOriginalTables:mutable.HashSet[AssociationIdentifier],
    //                                                                key: collection.IndexedSeq[SurrogateAttributeLineage],
    //                                                                nonKeyAttribute:AttributeLineage,
    //                                                                foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
    //                                                                val surrogateBasedTemporalRows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRow] = collection.mutable.ArrayBuffer(),
    //                                                                uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID()
    val rows = collection.mutable.ArrayBuffer() ++ histories.zipWithIndex.map{case (vh,i) => new SurrogateBasedTemporalRow(IndexedSeq(i),FactLineage.fromSerializationHelper(vh.lineage),IndexedSeq())}
    val pk = SurrogateAttributeLineage(0,attrID)
    val attributeLineage = new AttributeLineage(attrID,collection.mutable.TreeMap())
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(id.compositeID,
      collection.mutable.HashSet(id),
      IndexedSeq[SurrogateAttributeLineage](pk),
      attributeLineage,
      IndexedSeq[SurrogateAttributeLineage](),
      rows)
  }

  def getFilenameForBucket(originalBucketFilename:String) = {
    val pageMin = BigInt(originalBucketFilename.split("xml-p")(1).split("p")(0))
    val pageMax = BigInt(originalBucketFilename.split("xml-p")(1).split("p")(1).split("\\.")(0))
    s"$pageMin-$pageMax.json"
  }
}
