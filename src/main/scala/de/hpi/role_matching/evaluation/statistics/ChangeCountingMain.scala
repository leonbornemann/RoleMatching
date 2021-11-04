package de.hpi.role_matching.evaluation.statistics

import de.hpi.role_matching.cbrm.data.{RoleLineage, Roleset}

object ChangeCountingMain extends App {
  val vertexLookupDir = args(0)
  val dsNames = Seq("politics", "military", "education", "football", "tv_and_film")
  val finalRes = dsNames.foreach(dsName => {
    val map = Roleset.fromJsonFile(vertexLookupDir + s"/$dsName.json")
    val nchanges = map.posToRoleLineage.values
      .toIndexedSeq
      .map(fl => {
        val withWildcards = fl.lineage.size - 1
        val nonWCValuesWithIndex = fl.lineage.toIndexedSeq
          .filter(t => !RoleLineage.isWildcard(t._2))
          .zipWithIndex
        val withoutWildcards = nonWCValuesWithIndex
          .filter { case ((t, v), i) => i == 0 || v != nonWCValuesWithIndex(i - 1) }
          .size - 1
        (withWildcards, withoutWildcards)
      })
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println(s"$dsName: $nchanges")
  })
}
