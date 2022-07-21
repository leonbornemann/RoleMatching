package de.hpi.role_matching.data

import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}

case class RoleMatchCandidateIds(v1:String, v2:String) extends JsonWritable[RoleMatchCandidateIds]{

}

object RoleMatchCandidateIds extends JsonReadable[RoleMatchCandidateIds]
