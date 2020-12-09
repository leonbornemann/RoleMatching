package de.hpi.dataset_versioning.db_synthesis.baseline.index

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class TupleGroup[A](chosenTimestamps:ArrayBuffer[LocalDate], valuesAtTimestamps:IndexedSeq[A], tuplesInNode:Iterable[TupleReference[A]], wildcardTuples:Iterable[TupleReference[A]]) {

}
