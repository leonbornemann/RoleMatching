package de.hpi.role_matching.cbrm.relaxed_compatibility

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}

case class RoleAsDomain(ID:String,Values:IndexedSeq[String]) extends JsonWritable[RoleAsDomain]{

}

object RoleAsDomain extends JsonReadable[RoleAsDomain]
