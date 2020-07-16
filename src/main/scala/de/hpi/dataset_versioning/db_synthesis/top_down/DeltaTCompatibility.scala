package de.hpi.dataset_versioning.db_synthesis.top_down

import java.time.Duration

import de.hpi.dataset_versioning.data.change.{Change, FieldLineageCompatibility, FieldLineageReference, ReservedChangeValues}

class DeltaTCompatibility(delta_t_in_days: Int) extends FieldLineageCompatibility{

  override def isCompatible(fl1: FieldLineageReference, fl2: FieldLineageReference): Boolean = {
    val it1 = fl1.lineage.lineage.iterator
    val it2 = fl2.lineage.lineage.iterator
    //TODO: jump to latest insert
    val insertTimeT1 = fl1.insertTime
    val insertTimeT2 = fl2.insertTime

    val curElem1 = it1.next()
    val curElem2 = it2.next()
    assert(curElem1._2 != ReservedChangeValues.NOT_EXISTANT)
    assert(curElem2._1 != ReservedChangeValues.NOT_EXISTANT)
    //TODO:
    var done = false
    while(!done){
      //TODO we don't know what we want exactly here yet
      if(curElem1!=curElem2){
        val (t1,elem1) = curElem1
        val (t2,elem2) = curElem1
        val timeDiff = Math.abs(t1.toEpochDay - t2.toEpochDay)
        if(timeDiff>delta_t_in_days){
          //we can early abort here unless we are in the timespan of the first value
        }
      } else{
        //no more disagreement, continue
      }
    }
    ???
  }
}
