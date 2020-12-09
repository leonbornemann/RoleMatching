package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.{SurrogateBasedSynthesizedTemporalDatabaseTableAssociation, SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch}
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.{General_Many_To_Many_TupleMatching, TableUnionMatch}

import scala.collection.mutable

case class ExecutedTableUnion(matchForSketch: TableUnionMatch[Int],
                              unionedTableSketch: SurrogateBasedSynthesizedTemporalDatabaseTableAssociationSketch,
                              sketchTupleMapping: mutable.HashMap[General_Many_To_Many_TupleMatching[Int], Int],
                              matchForSynthUnion: TableUnionMatch[Any],
                              unionedSynthTable: SurrogateBasedSynthesizedTemporalDatabaseTableAssociation,
                              synthTupleMapping: mutable.HashMap[General_Many_To_Many_TupleMatching[Any], Int]) {


}
