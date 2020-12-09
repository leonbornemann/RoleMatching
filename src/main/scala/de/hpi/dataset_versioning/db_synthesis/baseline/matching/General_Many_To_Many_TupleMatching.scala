package de.hpi.dataset_versioning.db_synthesis.baseline.matching

case class General_Many_To_Many_TupleMatching[A](tupleReferences: Seq[TupleReference[A]],score:Int) {

  //TODO:calculate score if necessary
}
