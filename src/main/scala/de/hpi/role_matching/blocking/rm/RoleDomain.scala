package de.hpi.role_matching.blocking.rm

import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}

case class RoleDomain(ID:String, Values:IndexedSeq[String]) extends JsonWritable[RoleDomain]{

}

object RoleDomain extends JsonReadable[RoleDomain]
