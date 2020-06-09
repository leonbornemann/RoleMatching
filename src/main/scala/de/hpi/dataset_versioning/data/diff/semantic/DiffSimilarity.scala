package de.hpi.dataset_versioning.data.diff.semantic

import com.google.gson.{JsonElement, JsonPrimitive}

case class DiffSimilarity(schemaSimilarity:Double=0.0, newValueSimilarity:Double=0.0, deletedValueSimilarity:Double=0.0, fieldUpdateSimilarity:Double=0.0,
                          newValueOverlap:Set[Any]=Set(),
                          oldValueOverlap:Set[Any]=Set(),
                          updateOverlap:Set[(Any,Any)] = Set()) {

}
