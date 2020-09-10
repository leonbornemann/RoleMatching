package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics

import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.TableUnionMatch
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.DecomposedTemporalTableIdentifier
import de.hpi.dataset_versioning.db_synthesis.sketches.{SynthesizedTemporalDatabaseTableSketch, TemporalColumnTrait}

trait TemporalDatabaseTableTrait[A] {
  def isTrueUnion =getUnionedTables.size>1


  def primaryKeyIsValid :Boolean

  def informativeTableName :String

  def nrows: Int

  def getID:String

  def getUnionedTables:collection.Set[DecomposedTemporalTableIdentifier]

  def columns :IndexedSeq[TemporalColumnTrait[A]]

  def primaryKey:collection.Set[AttributeLineage]

  def nonKeyAttributeLineages:collection.IndexedSeq[AttributeLineage]

  def executeUnion(other: TemporalDatabaseTableTrait[A],bestMatch: TableUnionMatch[A]) :TemporalDatabaseTableTrait[A]

}
