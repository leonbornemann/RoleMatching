package de.hpi.role_matching.clique_partitioning

import de.hpi.socrata.{JsonReadable, JsonWritable}

case class RoleMerge(clique:collection.Set[Int], cliqueScore:Double) extends JsonWritable[RoleMerge]{

  override def toString: String = "{" + clique.toIndexedSeq.sorted.mkString(",") + "}" + (cliqueScore)

}
object RoleMerge extends JsonReadable[RoleMerge]
