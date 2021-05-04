package de.hpi.tfm.evaluation.data

import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.data.socrata.{JsonReadable, JsonWritable}
import de.hpi.tfm.evaluation.wikipediaStyle.GeneralEdgeStatRow

case class GeneralEdge(v1:IdentifiedFactLineage, v2:IdentifiedFactLineage) extends JsonWritable[GeneralEdge] {

  def toGeneralEdgeStatRow(i: Int, trainGraphConfig: GraphConfig) = {
    GeneralEdgeStatRow(1,trainGraphConfig,v1.id,v2.id,v1.factLineage.toFactLineage,v2.factLineage.toFactLineage)
  }

}

object GeneralEdge extends JsonReadable[GeneralEdge]
