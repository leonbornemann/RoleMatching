package de.hpi.tfm.data.tfmp_input

import de.hpi.tfm.data.tfmp_input.association.AssociationSchema

import scala.collection.mutable

object GlobalSurrogateRegistry {

  private var curSurrogateIDCounter = 0

  def initSurrogateIDCounters(associations: IndexedSeq[AssociationSchema]) = {
    curSurrogateIDCounter = associations.map(_.surrogateKey.surrogateID).max + 1
  }

  val sketchSurrogateCounters = mutable.HashMap[Int, Int]()
  val surrogateCounters = mutable.HashMap[Int, Int]()

  def counterIsInitialized(surrogateID: Int, bool: Boolean): Boolean = {
    val map = if (bool) sketchSurrogateCounters else surrogateCounters
    map.contains(surrogateID)
  }

  def getNextFreeSurrogateID = {
    curSurrogateIDCounter += 1
    curSurrogateIDCounter - 1
  }


}
