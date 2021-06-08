package de.hpi.tfm.evaluation.data

import de.hpi.tfm.compatibility.graph.fact.IDBasedTupleReference
import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.{FactLineage, FactLineageWithHashMap, SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedTemporalRow, ValueTransition}
import de.hpi.tfm.data.wikipedia.infobox.transformed.WikipediaInfoboxValueHistory
import de.hpi.tfm.evaluation.data.IdentifiedFactLineage.digitRegex
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG

case class IdentifiedFactLineage(id:String, factLineage: FactLineageWithHashMap) extends JsonWritable[IdentifiedFactLineage] {

  def isNumeric = {
    factLineage.lineage.values.forall(v => FactLineage.isWildcard(v) || GLOBAL_CONFIG.nonInformativeValues.contains(v) || v.toString.matches(digitRegex))
  }

}

object IdentifiedFactLineage extends JsonReadable[IdentifiedFactLineage] {

  val digitRegex = "[0-9]+"

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

  def getTransitionHistogramForTFIDFFromVertices(vertices:Seq[IdentifiedFactLineage],granularityInDays:Int) :Map[ValueTransition[Any],Int] = {
    val allTransitions = vertices
      .flatMap( (v:IdentifiedFactLineage) => {
        val transitions = v.factLineage.toFactLineage.getValueTransitionSet(true,granularityInDays).toSeq
        transitions
      })
    allTransitions
      .groupBy(identity)
      .map(t => (t._1,t._2.size))
  }

}
