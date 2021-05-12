package de.hpi.tfm.evaluation.data

import de.hpi.tfm.compatibility.graph.fact.IDBasedTupleReference
import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, FactLineageWithHashMap, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow}
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory

case class IdentifiedFactLineage(id:String, factLineage: FactLineageWithHashMap) extends JsonWritable[IdentifiedFactLineage] {

}

object IdentifiedFactLineage extends JsonReadable[IdentifiedFactLineage] {

  def toAssociationTable(histories: IndexedSeq[FactLineage], id:AssociationIdentifier,attrID:Int) = {
    //id:String,
    //                                                                unionedOriginalTables:mutable.HashSet[AssociationIdentifier],
    //                                                                key: collection.IndexedSeq[SurrogateAttributeLineage],
    //                                                                nonKeyAttribute:AttributeLineage,
    //                                                                foreignKeys:collection.IndexedSeq[SurrogateAttributeLineage],
    //                                                                val surrogateBasedTemporalRows:collection.mutable.ArrayBuffer[SurrogateBasedTemporalRow] = collection.mutable.ArrayBuffer(),
    //                                                                uniqueSynthTableID:Int = SynthesizedDatabaseTableRegistry.getNextID()
    val rows = collection.mutable.ArrayBuffer() ++ histories.zipWithIndex.map{case (vh,i) => new SurrogateBasedTemporalRow(IndexedSeq(i),vh,IndexedSeq())}
    val pk = SurrogateAttributeLineage(0,attrID)
    val attributeLineage = new AttributeLineage(attrID,collection.mutable.TreeMap())
    new SurrogateBasedSynthesizedTemporalDatabaseTableAssociation(id.compositeID,
      collection.mutable.HashSet(id),
      IndexedSeq[SurrogateAttributeLineage](pk),
      attributeLineage,
      IndexedSeq[SurrogateAttributeLineage](),
      rows)
  }

  def getIDString(subdomain:String,id:IDBasedTupleReference) = {
    subdomain +"_"+ id.toString
  }

}
