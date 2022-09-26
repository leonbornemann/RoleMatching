package de.hpi.role_matching.data

import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}

case class LabelledRoleMatchCandidate(id1:String,id2:String,isTrueRoleMatch:Boolean) extends JsonWritable[LabelledRoleMatchCandidate]

object LabelledRoleMatchCandidate extends JsonReadable[LabelledRoleMatchCandidate]
