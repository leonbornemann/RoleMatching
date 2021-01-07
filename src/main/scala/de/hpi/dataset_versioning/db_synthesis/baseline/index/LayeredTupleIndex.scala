package de.hpi.dataset_versioning.db_synthesis.baseline.index

import com.typesafe.scalalogging.StrictLogging
import de.hpi.dataset_versioning.db_synthesis.baseline.config.GLOBAL_CONFIG
import de.hpi.dataset_versioning.db_synthesis.baseline.database.TemporalDatabaseTableTrait
import de.hpi.dataset_versioning.db_synthesis.baseline.matching.TupleReference

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class LayeredTupleIndex[A](val chosenTimestamps: ArrayBuffer[LocalDate],
                           associationsWithColumnIndex: collection.Set[(TemporalDatabaseTableTrait[A],Int)],
                           val skipZeroChangeTuples:Boolean=true) extends StrictLogging{

  //currently
  def numLeafNodes = rootNode.children.size

  def serializeDetailedStatistics() = {
    val file = new File("indexStats.csv")
    val pr = new PrintWriter(file)
    pr.println("NodeID,Timestamps,Node Values,#tables,#tuples,MostTuples_Rank_1,MostTuples_Rank_2,MostTuples_Rank_3,MostTuples_Rank_4,MostTuples_Rank_5")
    val it = tupleGroupIterator
    var id = 0
    logger.debug(s"Found ${wildCardBucket.size} Wildcards that need to be considered in every bucket")
    it.foreach{case g =>
      val bytable = g.tuplesInNode.groupBy(_.table)
        .map(t => (t._1,t._2.map(_.rowIndex)))
      val top5TupleCounts = bytable.toIndexedSeq.sortBy(-_._2.size)
        .take(5)
        .toBuffer
        .map((a: (TemporalDatabaseTableTrait[A], Iterable[Int])) => a._2.size)
      while(top5TupleCounts.size<5) top5TupleCounts.append(0)
      pr.println(s"$id,${chosenTimestamps.mkString(";")},${g.valuesAtTimestamps.mkString(";")},${bytable.keySet.size},${bytable.values.map(_.size).sum},${top5TupleCounts.mkString(",")}")
      id+=1
      if(id%100000==0)
        logger.debug(s"Iterated through $id index leaf nodes")
    }
    pr.close()
  }


  val wildCardBucket = collection.mutable.ArrayBuffer[TupleReference[A]]()

  def tupleGroupIterator :Iterator[TupleGroup[A]] = {
    new TupleGroupIterator()
  }

  //build the index
  val rootNode = new LayeredTupleIndexNode[A](None,null,false)
  for( (table,colIndex) <- associationsWithColumnIndex){
    for (rowIndex <- 0 until table.nrows)  {
      val observedChanges = table.getDataTuple(rowIndex).head.countChanges(GLOBAL_CONFIG.NEW_CHANGE_COUNT_METHOD)._1
      if(observedChanges>0){
        val allValuesAreWildcards = chosenTimestamps.forall(ts => table.fieldIsWildcardAt(rowIndex,colIndex,ts))
        if(allValuesAreWildcards) {
          wildCardBucket.addOne(TupleReference(table,rowIndex))
        } else{
          rootNode.insert(table,rowIndex,colIndex,chosenTimestamps)
        }
      }
    }
  }
  //TODO: now the index is built, we should implement querying functions! and test the index I guess
  class TupleGroupIterator() extends Iterator[TupleGroup[A]] {
    val treeIterator = rootNode.iterator

    override def hasNext: Boolean = treeIterator.hasNext

    override def next(): TupleGroup[A] = {
      val (keys,nextCollection) = treeIterator.next()
      TupleGroup(chosenTimestamps,keys,nextCollection,wildCardBucket)
    }
  }

}
