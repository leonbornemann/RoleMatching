package de.hpi.tfm.compatibility.graph.fact

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.evaluation.data.GeneralEdge

import java.io.{File, PrintWriter}
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContextExecutor, Future}

class ParallelBatchExecutor[A](val futures:java.util.concurrent.ConcurrentHashMap[Future[String], Boolean],
                            val context:ExecutionContextExecutor,
                            val resultDir:File,
                            val toGeneralEdgeFunction:((TupleReference[A],TupleReference[A]) => GeneralEdge)) extends StrictLogging {

  var curCandidateBuffer = scala.collection.mutable.ArrayBuffer[(TupleReference[A],TupleReference[A])]()
  val batchSize = 10000
  var batchCounter = 0

  def addCandidateAndMaybeCreateBatch(ref1: TupleReference[A], ref2: TupleReference[A]) = {
    curCandidateBuffer.append((ref1,ref2))
    if(curCandidateBuffer.size>=batchSize){
      //get a new future computation:
      startCurrentBatchIfNotEmpty()
    }
  }

  def getTupleMatchOption(ref1:TupleReference[A], ref2:TupleReference[A]) = {
    val left = ref1.getDataTuple.head
    val right = ref2.getDataTuple.head // this is a map with all LHS being fields from tupleA and all rhs being fields from tuple B
    val evidence = left.getOverlapEvidenceCount(right)
    if (evidence == -1) {
      None
    } else {
      Some(FactMatch(ref1,ref2, evidence))
    }
  }

  def computeAll(curCandidateBuffer: ArrayBuffer[(TupleReference[A], TupleReference[A])], resultDir: File, i: Int): String = {
    val pr:PrintWriter = new PrintWriter(resultDir.getAbsolutePath + s"/$i.json")
    curCandidateBuffer.foreach{case (tr1,tr2) => {
      val option = getTupleMatchOption(tr1,tr2)
      if(option.isDefined){
        val e = option.get
        val edge = toGeneralEdgeFunction(e.tupleReferenceA,e.tupleReferenceB)
        edge.appendToWriter(pr,false,true)
      }
    }}
    pr.close()
    s"Done with Batch ${i}"
  }

  def createNewBatch(i:Int) = {
    logger.debug(s"Creating Batch $i")
    val f = Future{ computeAll(curCandidateBuffer,resultDir,i) }(context)
    futures.put(f,true)
    f.onComplete(_ => {
      futures.remove(f)
      logger.debug(s"Finished batch $i - Remaining computations: ${futures.size()}")
    })(context)
    f
  }

  def startCurrentBatchIfNotEmpty() = {
    if(!curCandidateBuffer.isEmpty){
      val batchID = batchCounter
      createNewBatch(batchID)
      curCandidateBuffer = scala.collection.mutable.ArrayBuffer[(TupleReference[A],TupleReference[A])]()
      batchCounter+=1
    }
  }

}
