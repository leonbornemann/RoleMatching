package de.hpi.dataset_versioning.db_synthesis.baseline.index

import java.time.LocalDate
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

import scala.collection.mutable.ArrayBuffer

case class TupleGroup[A](chosenTimestamps:ArrayBuffer[LocalDate], valuesAtTimestamps:IndexedSeq[A], tuplesInNode:Iterable[TupleReference[A]], wildcardTuples:Iterable[TupleReference[A]]) {

}
