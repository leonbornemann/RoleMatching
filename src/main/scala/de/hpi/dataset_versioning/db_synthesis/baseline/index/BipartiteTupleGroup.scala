package de.hpi.dataset_versioning.db_synthesis.baseline.index

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

case class BipartiteTupleGroup[A](chosenTimestamps:ArrayBuffer[LocalDate],
                                  valuesAtTimestamps:IndexedSeq[A],
                                  wildcardTuplesLeft:IndexedSeq[TupleReference[A]],
                                  wildcardTuplesRight:IndexedSeq[TupleReference[A]],
                                  tuplesLeft:IndexedSeq[TupleReference[A]],
                                  tuplesRight:IndexedSeq[TupleReference[A]]) {

}
