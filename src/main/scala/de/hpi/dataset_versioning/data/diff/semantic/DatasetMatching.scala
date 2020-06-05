package de.hpi.dataset_versioning.data.diff.semantic

import de.hpi.dataset_versioning.data.DatasetInstance

import scala.collection.mutable

class DatasetMatching() {
  val deletes = mutable.HashSet[DatasetInstance]()
  val matchings = mutable.HashMap[DatasetInstance,DatasetInstance]()
  val inserts = mutable.HashSet[DatasetInstance]()


}
