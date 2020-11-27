package de.hpi.dataset_versioning.data.simplified

import java.io.{File, PrintWriter}
import java.time.LocalDate

import de.hpi.dataset_versioning.data.DatasetInstance
import de.hpi.dataset_versioning.data.simplified.TransformToImprovedMain.id
import de.hpi.dataset_versioning.io.IOService

object TransformToImprovedMain extends App {
  IOService.socrataDir = args(0)
  val id = args(1)
  //val fromErrorFile = args.length >2 && args(2).toBoolean
  val timeRange = if(args.length > 2) Some(LocalDate.parse(args(2)),LocalDate.parse(args(3))) else None
  val transformer = new Transformer()
  if(id=="all")
    transformer.transformAll(timeRange)
  else
    transformer.transformAllForID(id,timeRange)

}
