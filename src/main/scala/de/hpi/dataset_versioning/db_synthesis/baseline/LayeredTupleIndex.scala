package de.hpi.dataset_versioning.db_synthesis.baseline

import java.time.LocalDate

import de.hpi.dataset_versioning.db_synthesis.baseline.heuristics.TemporalDatabaseTableTrait

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class LayeredTupleIndex[A](chosenTimestamps: ArrayBuffer[LocalDate],
                           associationsWithColumnIndex: collection.Set[(TemporalDatabaseTableTrait[A],Int)]) {

  val allTsWildcardBucket = collection.mutable.ArrayBuffer[(TemporalDatabaseTableTrait[A],Int)]()

  def tupleGroupIterator :Iterator[Iterable[(TemporalDatabaseTableTrait[A],Int)]] = {
    new TupleGroupIterator()
  }

  //build the index
  val rootNode = new LayeredTupleIndexNode[A](false)
  for( (table,colIndex) <- associationsWithColumnIndex){
    for (rowIndex <- 0 until table.nrows)  {
      val allValuesAreWildcards = chosenTimestamps.forall(ts => table.fieldIsWildcardAt(rowIndex,colIndex,ts))
      if(allValuesAreWildcards) {
        allTsWildcardBucket.addOne((table,rowIndex))
      } else{
        rootNode.insert(table,rowIndex,colIndex,chosenTimestamps)
      }
    }
  }
  //TODO: now the index is built, we should implement querying functions! and test the index I guess
  class TupleGroupIterator() extends Iterator[Iterable[(TemporalDatabaseTableTrait[A],Int)]] {
    val treeIterator = rootNode.iterator

    override def hasNext: Boolean = treeIterator.hasNext

    override def next(): Iterable[(TemporalDatabaseTableTrait[A], Int)] = {
      val nextCollection = treeIterator.next()
      nextCollection ++ allTsWildcardBucket
    }
  }

}
