package de.hpi.dataset_versioning.oneshot

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.AssociationGraphEdge
import de.hpi.dataset_versioning.io.{DBSynthesis_IOService, IOService}

object AssociationGraphEdgeExplorationMain extends App {
  IOService.socrataDir = args(0)
  val edges = AssociationGraphEdge.fromJsonObjectPerLineFile(DBSynthesis_IOService.getAssociationGraphEdgeFile.getAbsolutePath)
  val sum = edges.map(_.minChangeBenefit).sum
  println(sum)
  println()
  val edgesBetweenDifferentViewTableColumns = edges.sortBy(-_.evidence)
    .filter(a => a.firstMatchPartner.viewID!=a.secondMatchPartner.viewID)
  println(edgesBetweenDifferentViewTableColumns.size)
  println()
  edgesBetweenDifferentViewTableColumns
    .take(100)
    .map(age => {
      val table1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(age.firstMatchPartner)
      val table2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(age.secondMatchPartner)
      (table1.nonKeyAttribute.nameSet,table2.nonKeyAttribute.nameSet)
    })
    .foreach(println(_))
  val sameColNameEdges = edges.filter(e => {
    val table1 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.firstMatchPartner)
    val table2 = SurrogateBasedSynthesizedTemporalDatabaseTableAssociation.loadFromStandardOptimizationInputFile(e.secondMatchPartner)
    (table1.nonKeyAttribute.nameSet==table2.nonKeyAttribute.nameSet)
  })
  println(sameColNameEdges.size)
//  edgesBetweenDifferentViewTableColumns.foreach(println(_))
//  print(edgesBetweenDifferentViewTableColumns.size)
}
