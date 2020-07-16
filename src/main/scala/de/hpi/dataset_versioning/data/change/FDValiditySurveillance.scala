package de.hpi.dataset_versioning.data.change

import scala.collection.mutable

class FDValiditySurveillance(val leftToRight:mutable.HashMap[Seq[Any],Seq[Any]] = mutable.HashMap[Seq[Any],Seq[Any]]()) {

  def isStillValid(left:Seq[Any],right:Seq[Any]) = {
    !leftToRight.contains(left) || leftToRight(left) == right
  }

  def addAssociation(left:Seq[Any],right:Seq[Any]) = {
    assert(!leftToRight.contains(left))
    leftToRight(left) = right
  }

  //TODO: how do we treat missing columns?


}
