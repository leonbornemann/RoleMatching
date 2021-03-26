package de.hpi.tfm.data.socrata.diff.semantic

import de.hpi.tfm.data.socrata.DatasetInstance

import scala.collection.mutable

class DatasetMatching() {
  val deletes = mutable.HashSet[DatasetInstance]()
  val matchings = mutable.HashMap[DatasetInstance,DatasetInstance]()
  val inserts = mutable.HashSet[DatasetInstance]()


}
