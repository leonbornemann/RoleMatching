package de.hpi.dataset_versioning.db_synthesis.baseline

class ViewTupleMappingTracker(val id: String, entityIds: collection.IndexedSeq[Long], attributeIDs: collection.IndexedSeq[Int]) {

  def update(newSynthTable: SynthesizedTemporalDatabaseTable, executedMatch: TableUnionMatch[Int]) = {
    if(!executedMatch.firstMatchPartner.isTrueUnion && executedMatch.firstMatchPartner.getUnionedTables.head.viewID==id){

    }
  }


  val attributeMapping = collection.mutable.HashMap[Int,SynthesizedAttribute]()

  case class SynthesizedAttribute(tableID:Int,attrID:Int)

}
