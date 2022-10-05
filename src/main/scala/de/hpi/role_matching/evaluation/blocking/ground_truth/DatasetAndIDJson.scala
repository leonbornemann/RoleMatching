package de.hpi.role_matching.evaluation.blocking.ground_truth

import de.hpi.role_matching.data.LabelledRoleMatchCandidate
import de.hpi.role_matching.data.json_serialization.{JsonReadable, JsonWritable}

case class DatasetAndIDJson(dataset: String, id1: String, id2: String) extends JsonWritable[DatasetAndIDJson]{
  def toLabelledCandidate(isTrueRoleMatch: Boolean): LabelledRoleMatchCandidate = LabelledRoleMatchCandidate(id1,id2,isTrueRoleMatch)

}
object DatasetAndIDJson extends JsonReadable[DatasetAndIDJson]
