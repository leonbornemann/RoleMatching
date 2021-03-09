package de.hpi.dataset_versioning.db_synthesis.optimization

import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference
import scalax.collection.GraphBase
import scalax.collection.edge.WLkUnDiEdge

import scala.collection.mutable

/***
 * https://en.wikipedia.org/wiki/Bron%E2%80%93Kerbosch_algorithm
 * @param g
 */
class BronKerboshAlgorithm(g:GraphBase[TupleReference[Any], WLkUnDiEdge]) {

  val maxCliques = scala.collection.mutable.ArrayBuffer[Set[TupleReference[Any]]]()

  def bronKerbosh(R: Set[BronKerboshAlgorithm.this.g.NodeT],
                  X: Set[BronKerboshAlgorithm.this.g.NodeT],
                  P: Set[BronKerboshAlgorithm.this.g.NodeT]):Unit = {
    if(P.isEmpty && X.isEmpty){
      maxCliques += R.map(_.value).toSet
    } else {
      //var r = R
      var x = X
      var p = P
      P.foreach(v => {
        v.neighbors
        bronKerbosh(R ++ Seq(v),p.intersect(v.neighbors),x.intersect(v.neighbors))
        p = p.removedAll(Seq(v))
        x = x ++ Set(v)
      })
    }
  }

  /***
   * uses degeneracy sorting
   * @param P
   */
  def bronKerboshTopLevel(P: Set[BronKerboshAlgorithm.this.g.NodeT]):Unit = {
    var x = Set[BronKerboshAlgorithm.this.g.NodeT]()
    var p = P
    P.toIndexedSeq
      .sortBy(_.neighbors.size)
      .foreach(v => {
        bronKerbosh(Set(v),P.intersect(v.neighbors),x.intersect(v.neighbors))
        p = p.removedAll(Seq(v))
        x = x ++ Set(v)
      })
  }

  def run() = {
    bronKerboshTopLevel(Set() ++ g.nodes)
    maxCliques
  }
}
