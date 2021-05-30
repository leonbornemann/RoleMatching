package de.hpi.tfm.compatibility.graph

import scala.concurrent.Future
import scala.concurrent._


class FutureComputation(i:Int) {

  def slowFunction(i: Int) = { Thread.sleep(2000); "hello" + i + "world" }

  def run(context: ExecutionContext) = {
    Future{ slowFunction(i) }(context)
  }

}
