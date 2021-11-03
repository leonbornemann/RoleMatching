package de.hpi.role_matching.evaluation.exploration

import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}

object SingleRoleMatchPrintMain extends App {
  val politicsRoleSet = Roleset.fromJsonFile(args(0))
  val ids = Seq("infobox officeholder||18310386||224027176-0||monarch_\uD83D\uDD17_extractedLink0",
    "infobox officeholder||30482243||408010739-0||monarch_\uD83D\uDD17_extractedLink0",
    "infobox politician||3643178||214190977-0||monarch_\uD83D\uDD17_extractedLink0")
  val seqWithName = politicsRoleSet.positionToRoleLineage.values.toIndexedSeq.map(idfl => (idfl.csvSafeID,idfl)).toMap
  val results = ids.map(id => {
    seqWithName(id)
  })
  RoleLineageWithID.printTabularEventLineageString(results)

}
