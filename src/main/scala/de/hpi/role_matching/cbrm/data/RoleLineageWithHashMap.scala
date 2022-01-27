package de.hpi.role_matching.cbrm.data

import de.hpi.role_matching.cbrm.data.json_serialization.{JsonReadable, JsonWritable}

import java.time.LocalDate

case class RoleLineageWithHashMap(lineage: Map[LocalDate, Any]) extends JsonWritable[RoleLineageWithHashMap]{

  def toRoleLineage = {
    new RoleLineage(collection.mutable.TreeMap[LocalDate,Any]() ++ lineage)
  }

  def toFactLineage = RoleLineage.fromSerializationHelper(this)

}

object FactLineageWithHashMap extends JsonReadable[RoleLineageWithHashMap]