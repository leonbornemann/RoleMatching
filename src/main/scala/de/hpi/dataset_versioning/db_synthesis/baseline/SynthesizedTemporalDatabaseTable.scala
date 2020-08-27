package de.hpi.dataset_versioning.db_synthesis.baseline

import de.hpi.dataset_versioning.data.change.temporal_tables.{AttributeLineage, ProjectedTemporalRow, TemporalRow, TemporalTable}
import de.hpi.dataset_versioning.db_synthesis.baseline.decomposition.{DecomposedTemporalTable, DecomposedTemporalTableIdentifier}
import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.{PairwiseTupleMapper, TemporalDatabaseTableTrait, TemporalSchemaMapper}
import de.hpi.dataset_versioning.db_synthesis.baseline.index.ValueLineageIndex
import de.hpi.dataset_versioning.db_synthesis.bottom_up.ValueLineage
import de.hpi.dataset_versioning.db_synthesis.sketches.SynthesizedTemporalDatabaseTableSketch

import scala.collection.mutable

class SynthesizedTemporalDatabaseTable(val unionedTables:mutable.HashSet[DecomposedTemporalTableIdentifier],
                                       val schema: collection.IndexedSeq[AttributeLineage],
                                       val keyAttributeLineages: collection.Set[AttributeLineage],
                                       private val rows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer(),
                                       private var curEntityIDCounter:Long) extends TemporalDatabaseTableTrait{

  def tryUnion(tableB: SynthesizedTemporalDatabaseTable): Option[SynthesizedTemporalDatabaseTable] = {
    ???
//    val tableA = this
//    //enumerate all schema mappings:
//    val schemaMapper = new TemporalSchemaMapper()
//    val schemaMappings = schemaMapper.enumerateAllValidSchemaMappings(tableA,tableB)
//    var curBestScore = 0
//    var curBestMapping:collection.Map[Set[AttributeLineage], Set[AttributeLineage]] = null
//    for(mapping <- schemaMappings){
//      val (mappingToOverlap,attrIdToNonWildCardOverlapA,attrIdToNonWildCardOverlapB) = getNonWildCardOverlap(mapping)
//      if(mappingToOverlap.exists(!_._2.isEmpty)){
//        //we have a chance to save some changes here:
//        val mappingOrdered = mapping.toIndexedSeq
//        val (overlapColumnAttrIDOrderA,overlapColumnAttrIDOrderB) = mappingOrdered.map{case (left,right) =>
//          (left.toIndexedSeq.sortBy(_.attrId).map(_.attrId),right.toIndexedSeq.sortBy(_.attrId).map(_.attrId))}
//          .reduce((a,b) => (a._1++b._1,a._2 ++ b._2))
//        val indexA = ValueLineageIndex.buildIndex(sketchA,attrIdToNonWildCardOverlapA,overlapColumnAttrIDOrderA)
//        val indexB = ValueLineageIndex.buildIndex(sketchB,attrIdToNonWildCardOverlapB,overlapColumnAttrIDOrderB)
//        val tupleMapper = new PairwiseTupleMapper(sketchA,sketchB,indexA,indexB,mapping)
//        val tupleMapping = tupleMapper.mapGreedy()
//        val totalScore = tupleMapping.totalScore
//        if(totalScore > curBestScore){
//          curBestScore = totalScore
//          curBestMapping = mapping
//        }
//      } else{
//        throw new AssertionError("This match could have been caught earlier and avoided entirely")
//      }
//    }
//    if(curBestMapping==null)
//      new TableUnionMatch(tableA,tableB,None,0,true)
//    else
//      new TableUnionMatch(tableA,tableB,Some(curBestMapping),curBestScore,true)
  }

  def computeUnionMatch(other:SynthesizedTemporalDatabaseTable): Option[TableUnionMatch] = {
    //TODO: 1.load all data
    // 2. get best schema mapping given the data
    // 3. return that mapping
    ???
    //TODO: try out/test heuristic matching before doing this
  }


  def getActiveTime = {
    schema.map(_.activeTimeIntervals).reduce((a,b) => a.union(b))
  }

  def nonKeyAttributeLineages = schema.filter(al => !keyAttributeLineages.contains(al))


  var keyIsArtificial = false

  private var fieldAndRowMappings = mutable.HashMap[DecomposedTemporalTable,FieldAndRowMapping]()

  def getBestMergeMapping(curCandidate: DecomposedTemporalTable) = {
    val greedyMatcher = new SuperSimpleGreedySchemaMatcher(this,curCandidate)
    greedyMatcher.getBestSchemaMatching()
  }

  override def primaryKey = keyAttributeLineages
}
object SynthesizedTemporalDatabaseTable{

  def initFrom(dttToMerge: DecomposedTemporalTable) = {
    val synthesizedSchema = dttToMerge.containedAttrLineages
    var curEntityID:Long = 0
    val entityIDMatchingSynthesizedToOriginal = mutable.HashMap[Long,Long]()
    val tt = TemporalTable.loadAndCache(dttToMerge.id.viewID) //TODO: we will need to shrink this cache at some point
      .project(dttToMerge)
    val newRows:collection.mutable.ArrayBuffer[TemporalRow] = collection.mutable.ArrayBuffer()
    tt.projection.rows.foreach(tr => {
      val originalIds = mutable.HashMap[DecomposedTemporalTableIdentifier,Long](tt.projection.dttID.get.id -> tr.entityID)
      newRows.addOne(new SynthesizedTemporalRow(curEntityID,tr.fields,originalIds)) //TODO: use this
      entityIDMatchingSynthesizedToOriginal.put(curEntityID,tr.entityID)
      curEntityID +=1
    })
    val attributeMatchingSynthesizedToOriginal = synthesizedSchema.map(al => (al.attrId,al.attrId))
    val synthTable = new SynthesizedTemporalDatabaseTable(mutable.HashSet(dttToMerge.id),
      synthesizedSchema,
      dttToMerge.primaryKey,
      newRows,
      curEntityID)
    synthTable.fieldAndRowMappings.put(dttToMerge,new FieldAndRowMapping(attributeMatchingSynthesizedToOriginal,entityIDMatchingSynthesizedToOriginal))
    synthTable
  }

}