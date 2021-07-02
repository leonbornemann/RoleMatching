package de.hpi.socrata.matching

import de.hpi.socrata.io.Socrata_IOService

//this main unifies all attribute lineages, that have no temporal overlap but share a name at the "connection" points
//this should lead to less inserts/deletes and more updates
object ColumnMatchingRefinementMain extends App {
  Socrata_IOService.socrataDir = args(0)
  val id = args(1)
  val refiner = new ColumnMatchingRefinement(id)
  //refiner.refineAndMakeStateConsistent()

}
