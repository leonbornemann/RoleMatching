package de.hpi.role_matching.cbrm.data

import de.hpi.data_preparation.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.data_preparation.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.data_preparation.socrata.tfmp_input.table.nonSketch._
import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}
import de.hpi.role_matching.GLOBAL_CONFIG
import de.hpi.role_matching.cbrm.compatibility_graph.role_tree.IDBasedRoleReference
import de.hpi.util.TableFormatter

case class RoleLineageWithID(id:String, factLineage: FactLineageWithHashMap) extends JsonWritable[RoleLineageWithID] {

  def csvSafeID = id.replace('\r',' ').replace('\n',' ').replace(',',' ')

  def isNumeric = {
    factLineage.lineage.values.forall(v => FactLineage.isWildcard(v) || GLOBAL_CONFIG.nonInformativeValues.contains(v) || v.toString.matches(digitRegex))
  }

}

object RoleLineageWithID extends JsonReadable[RoleLineageWithID] {

  def printTabularEventLineageString(vertices:collection.Seq[RoleLineageWithID]) = {
    println(getTabularEventLineageString(vertices))
  }

  def getTabularEventLineageString(vertices:collection.Seq[RoleLineageWithID]):String = {
    val allDates = vertices.flatMap(_.factLineage.lineage.keySet).sortBy(_.toEpochDay)
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

  def getIDString(subdomain:String,id:IDBasedRoleReference) = {
    subdomain +"_"+ id.toString
  }

  def getTransitionHistogramForTFIDFFromVertices(vertices:Seq[RoleLineageWithID], granularityInDays:Int) :Map[ValueTransition[Any],Int] = {
    val allTransitions = vertices
      .flatMap( (v:RoleLineageWithID) => {
        val transitions = v.factLineage.toFactLineage.getValueTransitionSet(true,granularityInDays).toSeq
        transitions
      })
    allTransitions
      .groupBy(identity)
      .map(t => (t._1,t._2.size))
  }

}
