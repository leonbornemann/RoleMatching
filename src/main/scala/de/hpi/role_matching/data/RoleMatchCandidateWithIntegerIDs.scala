package de.hpi.role_matching.data

import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}

case class RoleMatchCandidateWithIntegerIDs(clique:collection.Set[Int], cliqueScore:Double) extends JsonWritable[RoleMatchCandidateWithIntegerIDs]{

  override def toString: String = "{" + clique.toIndexedSeq.sorted.mkString(",") + "}" + (cliqueScore)

}
object RoleMatchCandidateWithIntegerIDs extends JsonReadable[RoleMatchCandidateWithIntegerIDs]
