package de.hpi.tfm.data.tfmp_input.table

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.fact
import de.hpi.tfm.data.socrata.change.temporal_tables.attribute.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.tfm.data.tfmp_input.BinarySerializable
import de.hpi.tfm.data.tfmp_input.association.AssociationIdentifier
import de.hpi.tfm.data.tfmp_input.table.nonSketch.FactLineage
import de.hpi.tfm.io.DBSynthesis_IOService

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

  def writeToStandardTemporaryFile() = {
    val f = DBSynthesis_IOService.getSynthesizedTableTempFile(uniqueSynthTableID)
    writeToBinaryFile(f)
  }

  def tupleReferences = (0 until nrows).map(i => fact.TupleReference(this,i))

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
