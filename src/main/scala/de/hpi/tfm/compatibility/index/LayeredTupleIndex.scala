package de.hpi.tfm.compatibility.index

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.fact
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.data.tfmp_input.table.TemporalDatabaseTableTrait
import de.hpi.tfm.fact_merging.config.GLOBAL_CONFIG

import java.io.{File, PrintWriter}
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class LayeredTupleIndex[A](val chosenTimestamps: ArrayBuffer[LocalDate],
                           associationsWithColumnIndex: collection.Set[(TemporalDatabaseTableTrait[A],Int)],
                           val skipZeroChangeTuples:Boolean=true) extends IterableTupleIndex[A] with StrictLogging{
  assert(chosenTimestamps.size==1)
  assert(skipZeroChangeTuples)
  //currently
  def numLeafNodes = rootNode.children.size

  def serializeDetailedStatistics() = {
    val file = new File("indexStats.csv")
    val pr = new PrintWriter(file)
    pr.println("NodeID,Timestamps,Node Values,#tables,#tuples,MostTuples_Rank_1,MostTuples_Rank_2,MostTuples_Rank_3,MostTuples_Rank_4,MostTuples_Rank_5")
    val it = tupleGroupIterator(true)
    var id = 0
    logger.debug(s"Found ${allWildCardBuckets.map(_._2.size).sum} Wildcards that need to be considered in every bucket")
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


  val allWildCardBuckets = collection.mutable.HashMap[A,ArrayBuffer[TupleReference[A]]]()

  def tupleGroupIterator(skipWildCardBuckets: Boolean) :Iterator[TupleGroup[A]] = {
    assert(skipWildCardBuckets)
    if(!skipWildCardBuckets) throw new NotImplementedError()
    new TupleGroupIterator(skipWildCardBuckets)
  }

  //build the index
  val rootNode = new LayeredTupleIndexNode[A](None,null,false)
  for( (table,colIndex) <- associationsWithColumnIndex){
    for (rowIndex <- 0 until table.nrows)  {
      val observedChanges = table.getDataTuple(rowIndex).head.countChanges(GLOBAL_CONFIG.CHANGE_COUNT_METHOD)._1
      if(observedChanges>0){
        val valueAtLastChosenTs = table.fieldValueAtTimestamp(rowIndex,colIndex,chosenTimestamps.last)
        val lastValueIsWildcard = table.wildcardValues.contains(valueAtLastChosenTs)
        if(lastValueIsWildcard) {
          val buffer = allWildCardBuckets.getOrElseUpdate(valueAtLastChosenTs,ArrayBuffer[TupleReference[A]]())
          buffer.addOne(fact.TupleReference(table,rowIndex))
        } else{
          rootNode.insert(table,rowIndex,colIndex,chosenTimestamps)
        }
      }
    }
  }

  val allWildcards = allWildCardBuckets.values.toIndexedSeq.flatten
  //TODO: now the index is built, we should implement querying functions! and test the index I guess
  class TupleGroupIterator(skipWildCardBuckets: Boolean) extends Iterator[TupleGroup[A]] {
    val treeIterator = rootNode.iterator

    override def hasNext: Boolean = treeIterator.hasNext

    override def next(): TupleGroup[A] = {
      val (keys,nextCollection) = treeIterator.next()
      TupleGroup(chosenTimestamps,keys,nextCollection,allWildcards)
    }
  }

  override def wildcardBuckets: IndexedSeq[TupleGroup[A]] = allWildCardBuckets.map{case (k,values) => {
    TupleGroup(chosenTimestamps,IndexedSeq(k),IndexedSeq(),values)
  }}.toIndexedSeq

  override def getParentKeyValues = IndexedSeq()
}
