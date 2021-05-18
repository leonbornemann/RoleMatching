package de.hpi.tfm.evaluation.data

import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}

case class IdentifiedTupleMerge(clique:Set[String],cliqueScore:Double) extends JsonWritable[IdentifiedTupleMerge]{

}
object IdentifiedTupleMerge extends JsonReadable[IdentifiedTupleMerge]