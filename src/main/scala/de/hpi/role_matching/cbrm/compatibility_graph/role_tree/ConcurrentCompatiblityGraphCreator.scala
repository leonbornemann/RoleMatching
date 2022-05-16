package de.hpi.role_matching.cbrm.compatibility_graph.role_tree

import com.typesafe.scalalogging.StrictLogging
import de.hpi.role_matching.cbrm.compatibility_graph.GraphConfig
import de.hpi.role_matching.cbrm.compatibility_graph.representation.simple.SimpleCompatbilityGraphEdge
import de.hpi.role_matching.cbrm.data.{RoleReference, ValueTransition}

import java.io.{File, PrintWriter}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, Executors, Semaphore}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class ConcurrentCompatiblityGraphCreator(roles: IndexedSeq[RoleReference],
                                         graphConfig:GraphConfig,
                                         filterByCommonWildcardIgnoreChangeTransition:Boolean=true,
                                         nonInformativeValues:Set[Any] = Set(),
                                         nthreads:Int,
                                         resultDir:File,
                                         toGeneralEdgeFunction:((RoleReference,RoleReference) => SimpleCompatbilityGraphEdge),
                                         serializeGroupsOnly:Boolean=false
                             ) extends StrictLogging {

  logger.debug("Cleanung up old files")
  resultDir.listFiles().foreach(_.delete())
  logger.debug("Finished cleanup")

  private val service = Executors.newFixedThreadPool(nthreads)
  val context = ExecutionContext.fromExecutor(service)
  val futures = new java.util.concurrent.ConcurrentHashMap[String,Future[Any]]()

  var tupleToNonWcTransitions:Option[Map[RoleReference, Set[ValueTransition]]] = None
  logger.debug(s"processing ${roles.size} roles")
  var i = 0
  if(filterByCommonWildcardIgnoreChangeTransition){
    tupleToNonWcTransitions = Some(roles
      .map(t => {
        i+=1
        val role = t.getRole
        val valueTransitions = role
          .valueTransitions(false, true)
        val valueTransitionsFiltered = valueTransitions
          .filter(t => !nonInformativeValues.contains(t.prev) && !nonInformativeValues.contains(t.after))
        (t,valueTransitionsFiltered
        )
      })
      .toMap)
  }

  val fname = "graph"
  logger.debug("beginning future computation")
  ConcurrentCompatiblityGraphCreator.lastReportTimestamp = System.currentTimeMillis()
  AsynchronousRoleTree.createAsFuture(futures,roles,IndexedSeq(),IndexedSeq(),graphConfig,nonInformativeValues,context,resultDir,fname,toGeneralEdgeFunction,tupleToNonWcTransitions,0,true,serializeGroupsOnly)
  ConcurrentCompatiblityGraphCreator.allFuturesTerminated.acquire()
  ConcurrentCompatiblityGraphCreator.closeAllPrintWriters()
  logger.debug("Finished - closing print writers and shutting down executor service")

  assert(futures.size()==0)
  service.shutdownNow()

  //now we can shut everything down!

}
object ConcurrentCompatiblityGraphCreator extends StrictLogging {

  def closeAllPrintWriters() = {
    availablePrintWriters.synchronized{
      availablePrintWriters.foreach(t => {
        t._1.flush()
        t._1.close()
        println(s"Closed writer for ${t._2}")
      })
    }
    availablePrintWritersStats.synchronized{
      availablePrintWritersStats.foreach(t => {
        t._1.flush()
        t._1.close()
        println(s"Closed Stat writer for ${t._2}")
      })
    }
  }

  var lastReportTimestamp = System.currentTimeMillis()
  val logTimeDistanceInMs = 60000
  val reportInProgress = new AtomicBoolean(false)
  var outputFileCounter = 0
  var statFileCounter = 0
  val availablePrintWriters = scala.collection.mutable.ListBuffer[(PrintWriter,String)]()
  val availablePrintWritersStats = scala.collection.mutable.ListBuffer[(PrintWriter,String)]()
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

  def getOrCreateNewStatsPrintWriter(resultDirStats:File):(PrintWriter,String) = {
    availablePrintWritersStats.synchronized {
      if(availablePrintWritersStats.isEmpty){
        //        logger.debug(s"Created new Writer $outputFileCounter")
        val fname = s"partition_$statFileCounter.json"
        val newWriter = new PrintWriter(resultDirStats.getAbsolutePath + s"/$fname")
        statFileCounter += 1
        (newWriter,fname)
      } else {
        val res = availablePrintWritersStats.remove(0)
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
      pr.flush()
      availablePrintWriters.append((pr,filename))
    }
  }

  def releaseStatPrintWriter(pr:PrintWriter,filename:String) = {
    availablePrintWritersStats.synchronized {
      //      logger.debug(s"Released writer $filename")
      //      if(availablePrintWriters.exists(_._2==filename))
      //        logger.debug("Huh?")
      pr.flush()
      availablePrintWritersStats.append((pr,filename))
    }
  }

  import scala.util.{Failure, Success}

  def maybeReport(futures: ConcurrentHashMap[String, Future[Any]]) = {
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

  def checkTermination(futures: ConcurrentHashMap[String, Future[Any]]) = {
      this.synchronized {
        if(futures.size()==0){
          logger.debug("Overall program terminated - sending termination signal")
          allFuturesTerminated.release(1)
        }
    }
  }

  def setupFuture(f: Future[Any],
                     fname: String,
                     futures: ConcurrentHashMap[String, Future[Any]],
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
        //logger.debug(s"$fname - terminating")
        checkTermination(futures)
      }
      case Failure(e) => {
        e.printStackTrace
        futures.remove(fname)
        maybeReport(futures)
        //logger.debug(s"$fname - terminating")
        checkTermination(futures)
      }
    }(context)
  }

}
