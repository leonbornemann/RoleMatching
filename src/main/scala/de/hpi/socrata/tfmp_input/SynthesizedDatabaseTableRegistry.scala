package de.hpi.socrata.tfmp_input

object SynthesizedDatabaseTableRegistry {
  def getNextID() = {
    curFreeID += 1
    curFreeID - 1
  }

  var curFreeID = 0

}
