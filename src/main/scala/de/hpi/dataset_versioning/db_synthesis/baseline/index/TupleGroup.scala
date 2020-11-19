package de.hpi.dataset_versioning.db_synthesis.baseline.index

import java.time.LocalDate

import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait

import scala.collection.mutable.ArrayBuffer

case class TupleGroup[A](chosenTimestamps:ArrayBuffer[LocalDate],valuesAtTimestamps:IndexedSeq[A],tuplesInNode:Iterable[(TemporalDatabaseTableTrait[A], Int)],wildcardTuples:Iterable[(TemporalDatabaseTableTrait[A], Int)]) {

}
