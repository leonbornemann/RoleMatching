package de.hpi.role_matching.clique_partitioning

import de.hpi.socrata.{JsonReadable, JsonWritable}

case class IdentifiedTupleMerge(clique:collection.Set[Int],cliqueScore:Double) extends JsonWritable[IdentifiedTupleMerge]{

  override def toString: String = "{" + clique.toIndexedSeq.sorted.mkString(",") + "}" + (cliqueScore)

}
object IdentifiedTupleMerge extends JsonReadable[IdentifiedTupleMerge]
