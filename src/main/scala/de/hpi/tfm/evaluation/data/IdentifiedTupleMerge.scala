package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

case class IdentifiedTupleMerge(clique:collection.Set[Int],cliqueScore:Double) extends JsonWritable[IdentifiedTupleMerge]{

  override def toString: String = "{" + clique.toIndexedSeq.sorted.mkString(",") + "}" + (cliqueScore)

}
object IdentifiedTupleMerge extends JsonReadable[IdentifiedTupleMerge]
