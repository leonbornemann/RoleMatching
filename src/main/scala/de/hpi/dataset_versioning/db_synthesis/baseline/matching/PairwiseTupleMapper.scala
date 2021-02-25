package de.hpi.dataset_versioning.db_synthesis.baseline.matching

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.data.change.temporal_tables.attribute.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.database.surrogate_based.SurrogateBasedSynthesizedTemporalDatabaseTableAssociation
import de.hpi.dataset_versioning.db_synthesis.baseline.index.TupleSetIndex
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.field_graph.FieldLineageMatchGraph
import de.hpi.dataset_versioning.db_synthesis.sketches.field.TemporalFieldTrait

import java.time.LocalDate
import scala.collection.mutable

class PairwiseTupleMapper[A](tableA: TemporalDatabaseTableTrait[A], tableB: TemporalDatabaseTableTrait[A], mapping: collection.Map[Set[AttributeLineage], Set[AttributeLineage]]) extends StrictLogging {

  val aColsByID = tableA.dataColumns.map(c => (c.attrID,c)).toMap
  val bColsByID = tableB.dataColumns.map(c => (c.attrID,c)).toMap
  val insertTimeA = tableA.insertTime
  val insertTimeB = tableB.insertTime
  val mergedInsertTime = Seq(tableA.insertTime,tableB.insertTime).min
  val isSurrogateBased = tableA.isSurrogateBased
  if(tableA.isSurrogateBased) assert(tableB.isSurrogateBased)


  def getOptimalMapping() = {
    val tuples = IndexedSeq(tableA,tableB)
      .map(t => (0 until t.nrows).map( r => TupleReference(t,r)))
      .flatten
    val fieldLineageMatchGraph:FieldLineageMatchGraph[A] = new FieldLineageMatchGraph[A](tuples)
    val graphBasedTupleMapper = new GraphBasedTupleMapper(tuples,fieldLineageMatchGraph.edges)
    graphBasedTupleMapper.mapGreedy()
  }


  def mergeTupleSketches(mappedFieldLineages:collection.Map[TemporalFieldTrait[A], TemporalFieldTrait[A]]) = {
    mappedFieldLineages.map{case (a,b) => a.tryMergeWithConsistent(b)}.toSeq
  }

  def buildTuples(tupA: Int, tupB: Int) = {
    mapping.map{case (a,b) => {
      val lineagesA = a.toIndexedSeq
        .map(al => aColsByID(al.attrId).fieldLineages(tupA))
        .sortBy(_.lastTimestamp.toEpochDay)
      val lineagesB = b.toIndexedSeq
        .map(al => bColsByID(al.attrId).fieldLineages(tupB))
        .sortBy(_.lastTimestamp.toEpochDay)
      val aConcatenated = if(lineagesA.size==1) lineagesA.head else lineagesA.reduce((x,y) => x.mergeWithConsistent(y))
      val bConcatenated = if(lineagesB.size==1) lineagesB.head else lineagesB.reduce((x,y) => x.mergeWithConsistent(y))
      (aConcatenated,bConcatenated)
    }}
  }

  def countChanges(tuple: collection.Seq[TemporalFieldTrait[A]],insertTime:LocalDate) = {
    if(isSurrogateBased){
      GLOBAL_CONFIG.CHANGE_COUNT_METHOD.countFieldChangesSimple(tuple)
    } else {
      ???
      //tuple.map(_.countChanges(insertTime, GLOBAL_CONFIG.CHANGE_COUNT_METHOD)).sum
    }
  }
}
object PairwiseTupleMapper extends StrictLogging{
  logger.debug("Current implementiation allows for negative change-gain when having evidence >=0 - we will need to see if we want this!")
}
