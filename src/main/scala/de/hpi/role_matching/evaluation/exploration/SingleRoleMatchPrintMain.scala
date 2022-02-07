package de.hpi.role_matching.evaluation.exploration

import de.hpi.role_matching.cbrm.data.{RoleLineageWithID, Roleset}

object SingleRoleMatchPrintMain extends App {
  val roleset = Roleset.fromJsonFile(args(0))
//  val ids = Seq("infobox military conflict||6559445||270669226-0||partof_\uD83D\uDD17_extractedLink0",
//    "infobox military conflict||6559445||270669226-0||result_\uD83D\uDD17_extractedLink0")
  val ids = Seq("infobox military unit||27558125||379761173-0||caption","infobox military unit||27558125||379761173-0||unit_name")
  val seqWithName = roleset.positionToRoleLineage.values.toIndexedSeq.map(idfl => (idfl.csvSafeID,idfl)).toMap
  val results = ids.map(id => {
    seqWithName(id)
  })
  RoleLineageWithID.printTabularEventLineageString(results)

}
