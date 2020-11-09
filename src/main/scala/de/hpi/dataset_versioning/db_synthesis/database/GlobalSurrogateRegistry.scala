package de.hpi.dataset_versioning.db_synthesis.database

import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.surrogate_based.SurrogateBasedDecomposedTemporalTable
import de.hpi.dataset_versioning.db_synthesis.database.table.AssociationSchema

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GlobalSurrogateRegistry {

  private var curSurrogateIDCounter = 0

  def initSurrogateIDCounters(associations: IndexedSeq[AssociationSchema]) = {
    curSurrogateIDCounter = associations.map(_.surrogateKey.surrogateID).max+1
  }

  val sketchSurrogateCounters = mutable.HashMap[Int,Int]()
  val surrogateCounters = mutable.HashMap[Int,Int]()

  def counterIsInitialized(surrogateID: Int, bool: Boolean): Boolean = {
    val map = if(bool) sketchSurrogateCounters else surrogateCounters
    map.contains(surrogateID)
  }

  def getNextFreeSurrogateID = {
    curSurrogateIDCounter +=1
    curSurrogateIDCounter -1
  }


}
