package de.hpi.role_matching.cbrm.relaxed_compatibility

import de.hpi.role_matching.playground.{JsonReadable, JsonWritable}

case class RoleDomain(ID:String, Values:IndexedSeq[String]) extends JsonWritable[RoleDomain]{

}

object RoleDomain extends JsonReadable[RoleDomain]
