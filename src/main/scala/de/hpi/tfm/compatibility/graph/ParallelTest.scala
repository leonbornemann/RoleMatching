package de.hpi.tfm.compatibility.graph

import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

object ParallelTest extends App {

  private val service = Executors.newFixedThreadPool(2)
  val context = ExecutionContext.fromExecutor(service)

  val a = new FutureComputation(1).run(context)
  val b = new FutureComputation(2).run(context)
  val c = new FutureComputation(3).run(context)
  Seq(a,b,c).foreach(v =>
    println(Await.result(v, Duration.Inf))
  )
  println("done")
  service.shutdownNow()
}
