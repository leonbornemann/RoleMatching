package de.hpi.dataset_versioning.db_synthesis.optimization

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference
import scalax.collection.GraphBase
import scalax.collection.edge.WLkUnDiEdge

import scala.collection.mutable

class BronKerboshAlgorithm(g:GraphBase[TupleReference[Any], WLkUnDiEdge]) {

  val maxCliques = scala.collection.mutable.ArrayBuffer[Set[TupleReference[Any]]]()

  def bronKerbosh(R: Set[TupleReference[Any]], X: Set[TupleReference[Any]], P: Set[TupleReference[Any]]) = {
    ???
//    if(P.isEmpty && X.isEmpty){
//      maxCliques += R.toSet
//    } else {
//      //var r = R
//      var x = X
//      var p = P
//      P.foreach(v => {
//        bronKerbosh(R ++ Seq(v),)
//        p = p.removedAll(Seq(v))
//        x = x ++ Set(v)
//      })
//    }
  }

  def getAllCliques = {
    ???
//    val R = Set[TupleReference[Any]]()
//    val X = Set[TupleReference[Any]]()
//    val P = g.nodes.toSet
//    val p:BronKerboshAlgorithm.this.g.NodeT = g.nodes.head
//    bronKerbosh(R,X,P)
  }
}
