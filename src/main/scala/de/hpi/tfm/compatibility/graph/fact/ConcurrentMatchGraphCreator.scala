package de.hpi.tfm.compatibility.graph.fact

import com.typesafe.scalalogging.StrictLogging
import de.hpi.tfm.compatibility.GraphConfig
import de.hpi.tfm.compatibility.graph.fact.ConcurrentMatchGraphCreator.allFuturesTerminated
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreator
import de.hpi.tfm.compatibility.graph.fact.internal.InternalFactMatchGraphCreator.logger
import de.hpi.tfm.data.tfmp_input.table.nonSketch.ValueTransition
import de.hpi.tfm.data.wikipedia.infobox.fact_merging.FactMergingByTemplateMain.nthreads
import de.hpi.tfm.evaluation.data.GeneralEdge

import java.io.{File, PrintWriter}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ExecutorService, Executors, Semaphore, atomic}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

class ConcurrentMatchGraphCreator[A](tuples: IndexedSeq[TupleReference[A]],
                                  graphConfig:GraphConfig,
                                  filterByCommonWildcardIgnoreChangeTransition:Boolean=true,
                                  nonInformativeValues:Set[A] = Set[A](),
                                  nthreads:Int,
                                  resultDir:File,
                                  toGeneralEdgeFunction:((TupleReference[A],TupleReference[A]) => GeneralEdge),
                             ) extends StrictLogging {

  logger.debug("Cleanung up old files")
  resultDir.listFiles().foreach(_.delete())
  logger.debug("Finished cleanup")

  private val service = Executors.newFixedThreadPool(nthreads)
  val context = ExecutionContext.fromExecutor(service)
  val futures = new java.util.concurrent.ConcurrentHashMap[String,Future[FactMatchCreator[A]]]()

  var tupleToNonWcTransitions:Option[Map[TupleReference[A], Set[ValueTransition[A]]]] = None
  if(filterByCommonWildcardIgnoreChangeTransition){
    tupleToNonWcTransitions = Some(tuples
      .map(t => (t,t.getDataTuple.head
        .valueTransitions(false,true)
        .filter(t => !nonInformativeValues.contains(t.prev) && !nonInformativeValues.contains(t.after))
      ))
      .toMap)
  }

  val fname = "graph"
  ConcurrentMatchGraphCreator.lastReportTimestamp = System.currentTimeMillis()
  InternalFactMatchGraphCreator.createAsFuture(futures,tuples,IndexedSeq(),IndexedSeq(),graphConfig,nonInformativeValues,context,resultDir,fname,toGeneralEdgeFunction,tupleToNonWcTransitions,0)
  allFuturesTerminated.acquire()
  ConcurrentMatchGraphCreator.closeAllPrintWriters()
  logger.debug("Finished - closing print writers and shutting down executor service")

  assert(futures.size()==0)
  service.shutdownNow()

  //now we can shut everything down!

}
object ConcurrentMatchGraphCreator extends StrictLogging {

  def closeAllPrintWriters() = {
    availablePrintWriters.synchronized{
      availablePrintWriters.foreach(t => {
        t._1.close()
        println(s"Closed writer for ${t._2}")
      })
    }
  }

  var lastReportTimestamp = System.currentTimeMillis()
  val logTimeDistanceInMs = 10000
  val reportInProgress = new AtomicBoolean(false)
  var outputFileCounter = 0
  val availablePrintWriters = scala.collection.mutable.ListBuffer[(PrintWriter,String)]()
  val allFuturesTerminated = new Semaphore(0)

  def getOrCreateNewPrintWriter(resultDir:File):(PrintWriter,String) = {
    availablePrintWriters.synchronized {
      if(availablePrintWriters.isEmpty){
//        logger.debug(s"Created new Writer $outputFileCounter")
        val fname = s"partition_$outputFileCounter.json"
        val newWriter = new PrintWriter(resultDir.getAbsolutePath + s"/$fname")
        outputFileCounter += 1
        (newWriter,fname)
      } else {
        val res = availablePrintWriters.remove(0)
//        logger.debug(s"Acquired new print writer: ${res._2}")
        res
      }
    }
  }

  def releasePrintWriter(pr:PrintWriter,filename:String) = {
    availablePrintWriters.synchronized {
//      logger.debug(s"Released writer $filename")
//      if(availablePrintWriters.exists(_._2==filename))
//        logger.debug("Huh?")
      availablePrintWriters.append((pr,filename))
    }
  }

  import scala.util.{Success, Failure}

  def maybeReport[A](futures: ConcurrentHashMap[String, Future[FactMatchCreator[A]]]) = {
    val timeSinceLastReport = System.currentTimeMillis() - lastReportTimestamp
    if(timeSinceLastReport> logTimeDistanceInMs){
      if(!reportInProgress.get()){
        reportInProgress.set(true)
        logger.debug("--------------------------------------------------------------------------------------------------------------------------------")
        logger.debug(s"Currently active futures: ${futures.size()}")
        logger.debug("--------------------------------------------------------------------------------------------------------------------------------")
        this.synchronized {
          lastReportTimestamp = System.currentTimeMillis()
        }
        reportInProgress.set(false)
      }
    }
  }

  def checkTermination[A](futures: ConcurrentHashMap[String, Future[FactMatchCreator[A]]]) = {
      this.synchronized {
        if(futures.size()==0){
          logger.debug("Overall program terminated - sending termination signal")
          allFuturesTerminated.release(1)
        }
    }
  }

  def setupFuture[A](f: Future[FactMatchCreator[A]],
                     fname: String,
                     futures: ConcurrentHashMap[String, Future[FactMatchCreator[A]]],
                     context:ExecutionContextExecutor) = {
    if(futures.contains(fname)){
      logger.debug("ERROR,ERROR!!!")
      assert(false)
    }
    futures.put(fname,f)
    f.onComplete {
      case Success(value) => {
        futures.remove(fname)
        maybeReport(futures)
        logger.debug(s"$fname - terminating")
        checkTermination(futures)
      }
      case Failure(e) => {
        e.printStackTrace
        futures.remove(fname)
        maybeReport(futures)
        logger.debug(s"$fname - terminating")
        checkTermination(futures)
      }
    }(context)
  }

}
