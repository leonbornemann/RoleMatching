package de.hpi.role_matching.compatibility.graph.creation

import scala.math.Ordered.orderingToOrdered

case class FactMatch[A] private(tupleReferenceA:TupleReference[A],
                                tupleReferenceB: TupleReference[A],
                                var evidence:Int){
}
object FactMatch{
  def apply[A](tupleReferenceA:TupleReference[A],
               tupleReferenceB: TupleReference[A],
               score:Int): FactMatch[A] = {
    if(tupleReferenceA<=tupleReferenceB){
      new FactMatch[A](tupleReferenceA,tupleReferenceB,score)
    } else {
      new FactMatch[A](tupleReferenceB,tupleReferenceA,score)
    }
  }
}
