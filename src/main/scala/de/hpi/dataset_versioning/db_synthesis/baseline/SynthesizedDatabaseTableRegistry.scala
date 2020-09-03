package de.hpi.dataset_versioning.db_synthesis.baseline

object SynthesizedDatabaseTableRegistry {
  def getNextID() = {
   curFreeID+=1
   curFreeID-1
  }

  var curFreeID = 0

}
