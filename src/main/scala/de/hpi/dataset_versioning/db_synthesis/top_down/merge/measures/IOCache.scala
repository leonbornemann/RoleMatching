package de.hpi.dataset_versioning.db_synthesis.top_down.merge.measures

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.change.ChangeCube
import de.hpi.dataset_versioning.data.simplified.RelationalDataset
import de.hpi.dataset_versioning.db_synthesis.top_down.decomposition.normalization.DecomposedTable
import de.hpi.dataset_versioning.io.IOService

import scala.collection.mutable

object IOCache {
  def getOrLoadChanges(id: String) = {
    changeCubes.getOrElseUpdate(id,ChangeCube.load(id))
  }

  val datasets = mutable.HashMap[DatasetInstance,RelationalDataset]()
  val changeCubes = mutable.HashMap[String,ChangeCube]()

  def getOrLoadOriginalDataset(t: DecomposedTable) = {
    val instance = DatasetInstance(t.originalID, t.version)
    datasets.getOrElseUpdate(instance,IOService.loadSimplifiedRelationalDataset(instance))
  }

}
