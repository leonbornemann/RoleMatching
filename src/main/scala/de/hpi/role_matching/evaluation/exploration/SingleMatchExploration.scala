package de.hpi.role_matching.evaluation.exploration

import de.hpi.role_matching.compatibility.graph.representation.slim.VertexLookupMap
import de.hpi.role_matching.compatibility.graph.representation.vertex.IdentifiedFactLineage

object SingleMatchExploration extends App {
  val vertexLookupMap = VertexLookupMap.fromJsonFile("/home/leon/data/dataset_versioning/vertexLookupMaps/politics.json")
  val ids = Seq("infobox officeholder||18310386||224027176-0||monarch_\uD83D\uDD17_extractedLink0",
    "infobox officeholder||30482243||408010739-0||monarch_\uD83D\uDD17_extractedLink0",
    "infobox politician||3643178||214190977-0||monarch_\uD83D\uDD17_extractedLink0")
  val seqWithName = vertexLookupMap.posToLineage.values.toIndexedSeq.map(idfl => (idfl.csvSafeID,idfl)).toMap
  val results = ids.map(id => {
    seqWithName(id)
  })
  IdentifiedFactLineage.printTabularEventLineageString(results)

}
