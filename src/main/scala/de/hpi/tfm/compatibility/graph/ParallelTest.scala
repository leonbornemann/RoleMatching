package de.hpi.tfm.compatibility.graph

import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

object ParallelTest extends App {

  private val service = Executors.newFixedThreadPool(20)
  val context = ExecutionContext.fromExecutor(service)

  def createSet[T]() = {
      new java.util.concurrent.ConcurrentHashMap[T, Boolean]()
  }

  val set = createSet[Future[String]]()

  val seq = (0 until 20).map(i => {
    val f = new FutureComputation(i).get(context,set)
    f
  })
  val futureSet = seq.toSet
  println(s"Total number of computations: ${futureSet.size}")
  Thread.sleep(20)
  seq.foreach(v =>{
    Await.result(v, Duration.Inf)
    println("already done")
  })
  println("done - whoooopsie")
  service.shutdownNow()
}
