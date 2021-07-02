package de.hpi.socrata.tfmp_input.table

import com.typesafe.scalalogging.StrictLogging
import de.hpi.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.socrata.tfmp_input.BinarySerializable
import de.hpi.socrata.tfmp_input.association.AssociationIdentifier
import de.hpi.socrata.tfmp_input.table.nonSketch.FactLineage
import de.hpi.role_matching.compatibility.graph.creation

import java.time.LocalDate
import scala.collection.mutable

@SerialVersionUID(3L)
abstract class AbstractSurrogateBasedTemporalTable[A,B <: AbstractSurrogateBasedTemporalRow[A]](val id: String,
                                                                                                val unionedOriginalTables: mutable.HashSet[AssociationIdentifier],
                                                                                                val key: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                                                val nonKeyAttribute: AttributeLineage,
                                                                                                val foreignKeys: collection.IndexedSeq[SurrogateAttributeLineage],
                                                                                                val rows:collection.mutable.ArrayBuffer[B],
                                                                                                val uniqueSynthTableID: Int) extends  TemporalDatabaseTableTrait[A] with BinarySerializable with StrictLogging with Serializable{


  override def getKey: collection.IndexedSeq[SurrogateAttributeLineage] = key
  override def getUniqueSynthTableID: Int = uniqueSynthTableID
  override def getForeignKeys: collection.IndexedSeq[SurrogateAttributeLineage] = foreignKeys
  override def getNonKeyAttribute: AttributeLineage = nonKeyAttribute

  override def toString: String = unionedOriginalTables.mkString("_UNION_")+ s"(${key.mkString(",")}, ${nonKeyAttribute})"

  def isSketch: Boolean

  override def insertTime: LocalDate = rows
    .flatMap(_.value.getValueLineage
      .filter(t => !FactLineage.isWildcard(t._2))
      .map(_._1))
    .minBy(_.toEpochDay)

  def tupleReferences = (0 until nrows).map(i => creation.TupleReference(this,i))

  override def getDataTuple(rowIndex: Int): collection.IndexedSeq[TemporalFieldTrait[A]] = IndexedSeq(rows(rowIndex).value)

  override def primaryKeyIsValid: Boolean = ???

  override def informativeTableName: String = getID + "(" + key.mkString(",") + ",  " + nonKeyAttribute.lastName + ",  " + foreignKeys.mkString(",") + ")"

  override def nrows: Int = rows.size

  override def getID: String = id

  override def getUnionedOriginalTables: collection.Set[AssociationIdentifier] = unionedOriginalTables

  override def primaryKey: collection.Set[AttributeLineage] = ???

  override def dataAttributeLineages: collection.IndexedSeq[AttributeLineage] = IndexedSeq(nonKeyAttribute)

  override def isSurrogateBased: Boolean = true
}
