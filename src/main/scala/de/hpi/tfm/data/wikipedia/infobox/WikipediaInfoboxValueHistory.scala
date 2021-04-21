package de.hpi.tfm.data.wikipedia.infobox

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, FactLineageWithHashMap, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}

case class WikipediaInfoboxValueHistory(template:Option[String],pageID: BigInt, key: String, p: String, lineage: FactLineageWithHashMap) extends JsonWritable[WikipediaInfoboxValueHistory]{
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
}
