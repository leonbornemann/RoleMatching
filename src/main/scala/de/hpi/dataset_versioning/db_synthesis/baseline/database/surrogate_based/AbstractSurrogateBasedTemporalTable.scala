package de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based

import java.time.LocalDate

import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, SurrogateAttributeLineage}
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.BinarySerializable
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait
import de.hpi.dataset_versioning.io.DBSynthesis_IOService

import scala.collection.mutable

@SerialVersionUID(3L)
abstract class AbstractSurrogateBasedTemporalTable[A,B <: AbstractSurrogateBasedTemporalRow[A]](id: String,
                                                      unionedTables: mutable.HashSet[DecomposedTemporalTableIdentifier],
                                                      keys: collection.IndexedSeq[SurrogateAttributeLineage],
                                                      nonKeyAttribute: AttributeLineage,
                                                      foreignKeys: collection.IndexedSeq[SurrogateAttributeLineage],
                                                      rows:collection.mutable.ArrayBuffer[B],
                                                      uniqueSynthTableID: Int) extends  TemporalDatabaseTableTrait[A] with BinarySerializable with Serializable{

  override def insertTime: LocalDate = rows
    .flatMap(_.value.getValueLineage
      .filter(t => !ValueLineage.isWildcard(t._2))
      .map(_._1))
    .minBy(_.toEpochDay)

  def writeToStandardTemporaryFile() = {
    val f = DBSynthesis_IOService.getSynthesizedTableTempFile(uniqueSynthTableID)
    writeToBinaryFile(f)
  }

  override def getDataTuple(rowIndex: Int): collection.IndexedSeq[TemporalFieldTrait[A]] = IndexedSeq(rows(rowIndex).value)

  override def primaryKeyIsValid: Boolean = ???

  override def informativeTableName: String = getID + "(" + keys.mkString(",") + ",  " + nonKeyAttribute.lastName + ",  " + foreignKeys.mkString(",") + ")"

  override def nrows: Int = rows.size

  override def getID: String = id

  override def getUnionedTables: collection.Set[DecomposedTemporalTableIdentifier] = unionedTables

  override def primaryKey: collection.Set[AttributeLineage] = ???

  override def dataAttributeLineages: collection.IndexedSeq[AttributeLineage] = IndexedSeq(nonKeyAttribute)

  override def isSurrogateBased: Boolean = true

  override def executeUnion(other: TemporalDatabaseTableTrait[A], bestMatch: TableUnionMatch[A]): TemporalDatabaseTableTrait[A] = ???


}
