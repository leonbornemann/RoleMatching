package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.{HeuristicMatch, SynthesizedTemporalDatabaseTable, TimeIntervalSequence}
import de.hpi.dataset_versioning.db_synthesis.sketches.{DecomposedTemporalTableSketch, TemporalColumnSketch, Variant2Sketch}

import scala.collection.mutable

class SketchBasedHeuristicMatchCalculator extends HeursticMatchCalculator{

  val curSketches = mutable.HashMap[SynthesizedTemporalDatabaseTable,DecomposedTemporalTableSketch]()
  val schemaMapper = new TemporalSchemaMapper()

  def getOrLoadSketch(tableA: SynthesizedTemporalDatabaseTable) = {
    if(curSketches.contains(tableA))
      curSketches(tableA)
    else {
      assert(tableA.unionedTables.size==1)
      val dtt = tableA.unionedTables.head
      val sketch = DecomposedTemporalTableSketch.load(dtt,Variant2Sketch.getVariantName)
      sketch
    }
  }

  def getNonWildCardOverlapForMatchedAttributes(left: Set[AttributeLineage], right: Set[AttributeLineage]) = {
    val nonWildCardLeft = unionAll(left)
    val nonWildCardRight = unionAll(right)
    nonWildCardLeft.intersect(nonWildCardRight)
  }

  private def unionAll(left: Set[AttributeLineage]) = {
    if (left.size == 1) left.head.activeTimeIntervals else left.map(_.activeTimeIntervals).reduce((a, b) => {
      assert(a.intersect(b).isEmpty)
      val res = a.union(b)
      res
    })
  }

  def getNonWildCardOverlap(mapping: collection.Map[Set[AttributeLineage], Set[AttributeLineage]]) = {
    val mappingToOverlap = mapping.map(t => (t,getNonWildCardOverlapForMatchedAttributes(t._1,t._2)))
    val attrIdToNonWildCardOverlapLeft = mapping.flatMap(t => {
      val overlap = mappingToOverlap(t)
      t._1.map( al => (al.attrId,overlap.intersect(al.activeTimeIntervals)))
    })
    val attrIdToNonWildCardOverlapRight = mapping.flatMap(t => {
      val overlap = mappingToOverlap(t)
      t._2.map( al => (al.attrId,overlap.intersect(al.activeTimeIntervals)))
    })
    (mappingToOverlap,attrIdToNonWildCardOverlapLeft,attrIdToNonWildCardOverlapRight)
  }

  def indexByValueInNonWildcardPointInTime(sketchA: DecomposedTemporalTableSketch,
                                           sketchB: DecomposedTemporalTableSketch,
                                           mappingToOverlap: collection.Map[(Set[AttributeLineage], Set[AttributeLineage]), TimeIntervalSequence],
                                           attrIdToNonWildCardOverlapLeft: collection.Map[Int, TimeIntervalSequence],
                                           attrIdToNonWildCardOverlapRight: collection.Map[Int, TimeIntervalSequence],
                                           mapping: collection.Map[Set[AttributeLineage], Set[AttributeLineage]]) = {
    val mappingOrdered = mapping.toIndexedSeq
    val (overlapColumnAttrIDOrderLeft,overlapColumnAttrIDOrderLeftRight) = mappingOrdered.map{case (left,right) =>
      (left.toIndexedSeq.sortBy(_.attrId).map(_.attrId),right.toIndexedSeq.sortBy(_.attrId).map(_.attrId))}
      .reduce((a,b) => (a._1++b._1,a._2 ++ b._2))
    val colIDToColLeft = sketchA.temporalColumnSketches.map(c => (c.attrID,c)).toMap
    val columnsLeftInIndexOrder = overlapColumnAttrIDOrderLeft.map(colID => colIDToColLeft(colID))
    (0 until sketchA.temporalColumnSketches.head.fieldLineageSketches.size).foreach(tID => {
      val res = columnsLeftInIndexOrder.map(c => {
        //we need the nonWildCardOverlap of this column
        val timeToExtract = attrIdToNonWildCardOverlapLeft(c.attrID)
        val cActiveTime = c.attributeLineage.activeTimeIntervals
        c.fieldLineageSketches(tID).hashValuesAt(timeToExtract.intersect(cActiveTime)) //IMPORTANT: THESE ARE JUST THE HASH VALUES AT THIS TIME, NOT WILDCARDS BEFORE/AFTER
      })
    })
  }

  override def calculateMatch(tableA: SynthesizedTemporalDatabaseTable, tableB: SynthesizedTemporalDatabaseTable): HeuristicMatch = {
    val sketchA = getOrLoadSketch(tableA)
    val sketchB = getOrLoadSketch(tableB)
    //enumerate all schema mappings:
    val schemaMappings = schemaMapper.enumerateAllValidSchemaMappings(tableA,tableB)
    for(mapping <- schemaMappings){
      val (mappingToOverlap,attrIdToNonWildCardOverlapLeft,attrIdToNonWildCardOverlapRight) = getNonWildCardOverlap(mapping)
      if(mappingToOverlap.exists(!_._2.isEmpty)){
        //we have a chance to save some changes here:
        //TODO: index by the overlaps in each attr for both tables
        val index = indexByValueInNonWildcardPointInTime(sketchA,sketchB,mappingToOverlap,attrIdToNonWildCardOverlapLeft,attrIdToNonWildCardOverlapRight,mapping)
      } else{
        throw new AssertionError("This match could have been caught earlier and avoided entirely")
      }
      tableA.schema.map(_.activeTimeIntervals)
    }
    ???
  }
}
