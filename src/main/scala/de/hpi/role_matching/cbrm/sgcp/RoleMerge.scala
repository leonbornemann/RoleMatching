package de.hpi.role_matching.cbrm.sgcp

import de.hpi.data_preparation.socrata.{JsonReadable, JsonWritable}
import de.hpi.socrata.JsonReadable

case class RoleMerge(clique:collection.Set[Int], cliqueScore:Double) extends JsonWritable[RoleMerge]{

  override def toString: String = "{" + clique.toIndexedSeq.sorted.mkString(",") + "}" + (cliqueScore)

}
object RoleMerge extends JsonReadable[RoleMerge]
