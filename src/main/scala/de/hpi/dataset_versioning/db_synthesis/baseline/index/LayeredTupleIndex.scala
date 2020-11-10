package de.hpi.dataset_versioning.db_synthesis.baseline.index

import java.io.{File, PrintWriter}
import java.time.LocalDate

import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait

import scala.collection.mutable.ArrayBuffer

class LayeredTupleIndex[A](val chosenTimestamps: ArrayBuffer[LocalDate],
                           associationsWithColumnIndex: collection.Set[(TemporalDatabaseTableTrait[A],Int)]) {
  def serializeDetailedStatistics() = {
    val file = new File("indexStats.csv")
    val pr = new PrintWriter(file)
    pr.println("NodeID,Timestamps,Node Values,#tables,#tuples,MostTuples_Rank_1,MostTuples_Rank_2,MostTuples_Rank_3,MostTuples_Rank_4,MostTuples_Rank_5")
    val it = tupleGroupIterator
    var id = 0
    it.foreach{case (key,g) =>
      val bytable = g.groupBy(_._1)
        .map(t => (t._1,t._2.map(_._2)))
      val top5TupleCounts = bytable.toIndexedSeq.sortBy(-_._2.size)
        .take(5)
        .toBuffer
        .map((a: (TemporalDatabaseTableTrait[A], Iterable[Int])) => a._2.size)
      while(top5TupleCounts.size<5) top5TupleCounts.append(0)
      pr.println(s"$id,${chosenTimestamps.mkString(";")},${key.mkString(";")},${bytable.keySet.size},${bytable.values.map(_.size).sum},${top5TupleCounts.mkString(",")}")
      id+=1
    }
    pr.close()
  }


  val allTsWildcardBucket = collection.mutable.ArrayBuffer[(TemporalDatabaseTableTrait[A],Int)]()

  def tupleGroupIterator :Iterator[(IndexedSeq[A],Iterable[(TemporalDatabaseTableTrait[A], Int)])] = {
    new TupleGroupIterator()
  }

  //build the index
  val rootNode = new LayeredTupleIndexNode[A](None,null,false)
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
  class TupleGroupIterator() extends Iterator[(IndexedSeq[A],Iterable[(TemporalDatabaseTableTrait[A], Int)])] {
    val treeIterator = rootNode.iterator

    override def hasNext: Boolean = treeIterator.hasNext

    override def next(): (IndexedSeq[A],Iterable[(TemporalDatabaseTableTrait[A], Int)]) = {
      val (keys,nextCollection) = treeIterator.next()
      (keys,nextCollection ++ allTsWildcardBucket)
    }
  }

  def printDetailedInfo = {

  }

}
