package de.hpi.dataset_versioning.db_synthesis.database.query_tracking

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.database.SynthesizedTemporalDatabaseTable
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TableUnionMatch

class ViewTupleMappingTracker(val id: String, entityIds: collection.IndexedSeq[Long], attributeIDs: collection.IndexedSeq[Int]) {

  def update(newSynthTable: SynthesizedTemporalDatabaseTable, executedMatch: TableUnionMatch[Int]) = {
    if(!executedMatch.firstMatchPartner.isTrueUnion && executedMatch.firstMatchPartner.getUnionedTables.head.viewID==id){
      throw new AssertionError("query tracking implementation missing")
    }
  }


  val attributeMapping = collection.mutable.HashMap[Int,SynthesizedAttribute]()

  case class SynthesizedAttribute(tableID:Int,attrID:Int)

}
object ViewTupleMappingTracker extends StrictLogging{
  logger.debug("We are still missing mapping tracking implementations implementations")
}
