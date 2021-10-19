package de.hpi.role_matching.compatibility.graph.representation.vertex

import de.hpi.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.socrata.tfmp_input.table.nonSketch._
import de.hpi.socrata.{JsonReadable, JsonWritable}
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.compatibility.graph.creation.IDBasedTupleReference
import de.hpi.role_matching.compatibility.graph.representation.vertex.IdentifiedFactLineage.digitRegex
import de.hpi.util.TableFormatter

case class IdentifiedFactLineage(id:String, factLineage: FactLineageWithHashMap) extends JsonWritable[IdentifiedFactLineage] {

  def csvSafeID = id.replace('\r',' ').replace('\n',' ').replace(',',' ')

  def isNumeric = {
    factLineage.lineage.values.forall(v => FactLineage.isWildcard(v) || GLOBAL_CONFIG.nonInformativeValues.contains(v) || v.toString.matches(digitRegex))
  }

}

object IdentifiedFactLineage extends JsonReadable[IdentifiedFactLineage] {

  def printTabularEventLineageString(vertices:collection.Seq[IdentifiedFactLineage]) = {
    println(getTabularEventLineageString(vertices))
  }

  def getTabularEventLineageString(vertices:collection.Seq[IdentifiedFactLineage]):String = {
    val allDates = vertices.flatMap(_.factLineage.lineage.keySet)
    val header = Seq("") ++ allDates
    val cellsAll = vertices.map(v => {
      Seq(v.id) ++ allDates.map(t => v.factLineage.toFactLineage.valueAt(t)).map(v => if(FactLineage.isWildcard(v)) "_" else v)
    }).toSeq
    TableFormatter.format(Seq(header) ++ cellsAll)
  }


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
