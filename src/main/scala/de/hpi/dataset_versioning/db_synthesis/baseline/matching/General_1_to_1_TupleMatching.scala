package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait

import scala.math.Ordered.orderingToOrdered

case class General_1_to_1_TupleMatching[A] private (tupleReferenceA:TupleReference[A],
                                           tupleReferenceB: TupleReference[A],
                                           var score:Int){
}
object General_1_to_1_TupleMatching{
  def apply[A](tupleReferenceA:TupleReference[A],
               tupleReferenceB: TupleReference[A],
               score:Int): General_1_to_1_TupleMatching[A] = {
    if(tupleReferenceA<=tupleReferenceB){
      new General_1_to_1_TupleMatching[A](tupleReferenceA,tupleReferenceB,score)
    } else {
      new General_1_to_1_TupleMatching[A](tupleReferenceB,tupleReferenceA,score)
    }
  }
}
