package de.hpi.tfm.compatibility.index

import de.hpi.tfm.compatibility.graph.fact.TupleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class TupleGroup[A](chosenTimestamps:ArrayBuffer[LocalDate], valuesAtTimestamps:IndexedSeq[A], tuplesInNode:Iterable[TupleReference[A]], wildcardTuples:Iterable[TupleReference[A]]) {

}
