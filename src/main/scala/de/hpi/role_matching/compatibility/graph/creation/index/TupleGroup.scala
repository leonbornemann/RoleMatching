package de.hpi.role_matching.compatibility.graph.creation.index

import de.hpi.role_matching.compatibility.graph.creation.TupleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class TupleGroup[A](chosenTimestamps:ArrayBuffer[LocalDate], valuesAtTimestamps:IndexedSeq[A], tuplesInNode:Iterable[TupleReference[A]], wildcardTuples:Iterable[TupleReference[A]]) {

}
