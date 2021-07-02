package de.hpi.role_matching.compatibility.graph.creation

import scala.concurrent.{Future, _}


class FutureComputation(i:Int) {

  def slowFunction(i: Int) = { Thread.sleep(1000*(20-i)); "hello" + i + "world" }

  def get(context: ExecutionContext,set:java.util.concurrent.ConcurrentHashMap[Future[String],Boolean]) = {
    val f = Future{ slowFunction(i) }(context)
    set.put(f,true)
    f.onComplete(_ => {
      println(s"Done with $i")
      set.remove(f)
      println(s"Remaining computations: ${set.size()}")
    })(context)
    f
  }

}
