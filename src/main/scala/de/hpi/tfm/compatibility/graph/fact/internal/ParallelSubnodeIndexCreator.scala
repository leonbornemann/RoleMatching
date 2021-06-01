package de.hpi.tfm.compatibility.graph.fact.internal

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.graph.fact.TupleReference
import de.hpi.tfm.compatibility.index.{TupleGroup, TupleSetIndex}

import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContextExecutor, Future}

class ParallelSubnodeIndexCreator[A](subNodeFutures: ConcurrentHashMap[Future[String], Boolean],
                                  context: ExecutionContextExecutor,
                                  thresholdForParallelIndexCompute:Int) extends StrictLogging {

  var batchCounter = new AtomicInteger(0)

  def createFuture(tuples: IndexedSeq[TupleReference[A]],
                   parentDates: IndexedSeq[LocalDate],
                   parentValues: IndexedSeq[A],
                   wildcardKeyValues: Set[A],
                   matcher: InternalFactMatchGraphCreator[A],
                   recurseDepth: Int): Unit = {
    val curCounter = batchCounter.incrementAndGet()
    val f = Future {createNewIndexAndTraverse(tuples,parentDates, parentValues,wildcardKeyValues, matcher, recurseDepth,curCounter)}(context)
    f.onComplete(_ => {
      subNodeFutures.remove(f)
      logger.debug(s"Finished asynchronous indexing batch $curCounter - Remaining computations: ${subNodeFutures.size()}")
    })(context)
    subNodeFutures.put(f,true)
  }

  def createNewIndexAndTraverse(tuples: IndexedSeq[TupleReference[A]],
                                parentDates: IndexedSeq[LocalDate],
                                parentValues: IndexedSeq[A],
                                wildcardKeyValues: Set[A],
                                matcher: InternalFactMatchGraphCreator[A],
                                recurseDepth: Int,
                                curCounter:Int) = {
    val newIndexForSubNode = new TupleSetIndex[A](tuples,parentDates,parentValues,wildcardKeyValues,true)
    matcher.buildGraph(tuples,newIndexForSubNode,recurseDepth+1)
    s"Done with Subnode $curCounter"
  }


}
