package de.hpi.dataset_versioning.db_synthesis.baseline.heuristics
import de.hpi.dataset_versioning.data.change.temporal_tables.AttributeLineage
import de.hpi.dataset_versioning.db_synthesis.baseline.index.ValueLineageIndex
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
    val nonWildCardLeft = unionAllActiveTimes(left)
    val nonWildCardRight = unionAllActiveTimes(right)
    nonWildCardLeft.intersect(nonWildCardRight)
  }

  private def unionAllActiveTimes(lineages: Set[AttributeLineage]) = {
    if (lineages.size == 1) lineages.head.activeTimeIntervals else lineages.map(_.activeTimeIntervals).reduce((a, b) => {
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



  def createTupleMapping(indexA: ValueLineageIndex, indexB: ValueLineageIndex) = {

  }

  override def calculateMatch(tableA: SynthesizedTemporalDatabaseTable, tableB: SynthesizedTemporalDatabaseTable): HeuristicMatch = {
    val sketchA = getOrLoadSketch(tableA)
    val sketchB = getOrLoadSketch(tableB)
    //enumerate all schema mappings:
    val schemaMappings = schemaMapper.enumerateAllValidSchemaMappings(tableA,tableB)
    for(mapping <- schemaMappings){
      val (mappingToOverlap,attrIdToNonWildCardOverlapA,attrIdToNonWildCardOverlapB) = getNonWildCardOverlap(mapping)
      if(mappingToOverlap.exists(!_._2.isEmpty)){
        //we have a chance to save some changes here:
        //TODO: index by the overlaps in each attr for both tables
        val mappingOrdered = mapping.toIndexedSeq
        val (overlapColumnAttrIDOrderA,overlapColumnAttrIDOrderB) = mappingOrdered.map{case (left,right) =>
          (left.toIndexedSeq.sortBy(_.attrId).map(_.attrId),right.toIndexedSeq.sortBy(_.attrId).map(_.attrId))}
          .reduce((a,b) => (a._1++b._1,a._2 ++ b._2))
        val indexA = ValueLineageIndex.buildIndex(sketchA,attrIdToNonWildCardOverlapA,overlapColumnAttrIDOrderA)
        val indexB = ValueLineageIndex.buildIndex(sketchB,attrIdToNonWildCardOverlapB,overlapColumnAttrIDOrderB)
        val tupleMapper = new PairwiseTupleMapper(sketchA,sketchB,indexA,indexB,mapping)
        val tupleMapping = tupleMapper.mapGreedy()
      } else{
        throw new AssertionError("This match could have been caught earlier and avoided entirely")
      }
      tableA.schema.map(_.activeTimeIntervals)
    }
    ???
  }
}
