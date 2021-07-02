package de.hpi.socrata.diff.semantic

import de.hpi.socrata.DatasetInstance

import scala.collection.mutable

class DatasetMatching() {
  val deletes = mutable.HashSet[DatasetInstance]()
  val matchings = mutable.HashMap[DatasetInstance,DatasetInstance]()
  val inserts = mutable.HashSet[DatasetInstance]()


}
